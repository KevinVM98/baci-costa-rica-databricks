"""Comprobar rutas y escrituras sin conectarse a Unity Catalog ni Databricks."""

from pathlib import Path
import runpy
from types import SimpleNamespace
import zipfile

import pytest


NOTEBOOKS = Path(__file__).resolve().parents[1] / "baci_cr"


def cargar_configuracion(**variables):
    return runpy.run_path(str(NOTEBOOKS / "00_configuracion.py"), init_globals=variables)


def test_sin_parametro_se_elige_desarrollo():
    config = cargar_configuracion()
    assert config["ENTORNO"] == "dev"
    assert config["TABLA_COMERCIO"] == "workspace.baci_dev.comercio_enriquecido"


@pytest.mark.parametrize("entorno,esquema", [("dev", "baci_dev"), ("prod", "baci")])
def test_cambiar_salidas_no_mueve_los_insumos(entorno, esquema):
    config = cargar_configuracion(BACI_ENTORNO=entorno)
    for clave in ("TABLA_COMERCIO", "TABLA_CONCENTRACION", "TABLA_ALTERNATIVAS"):
        assert config[clave].startswith(f"workspace.{esquema}.")
    assert str(config["BASE_RAW"]) == "/Volumes/workspace/baci/raw"
    rutas = config["RUTAS_TRADE"] + [config["RUTA_PAISES"], config["RUTA_PRODUCTOS"]]
    assert len(config["RUTAS_TRADE"]) == 8
    assert all(ruta.startswith("/Volumes/workspace/baci/raw/") for ruta in rutas)


@pytest.mark.parametrize("entorno", ["", None, "DEV", "prod ", "staging", "baci; DROP SCHEMA baci"])
def test_entorno_invalido_falla_antes_de_acceder_a_datos(entorno):
    with pytest.raises(ValueError, match="Entorno inválido"):
        cargar_configuracion(BACI_ENTORNO=entorno)


@pytest.mark.parametrize("notebook", sorted(NOTEBOOKS.glob("0[1-5]_*.py")), ids=lambda p: p.stem)
@pytest.mark.parametrize("parametros,esquema", [({}, "baci_dev"), ({"entorno": "prod"}, "baci")])
def test_cada_entrada_lee_el_parametro_antes_de_cargar_configuracion(notebook, parametros, esquema):
    # Simula el contrato de widgets: un parámetro de la tarea tiene prioridad
    # sobre el valor inicial del widget. La integración real se prueba en DB.
    valores = dict(parametros)
    widgets = SimpleNamespace(
        dropdown=lambda nombre, inicial, opciones, etiqueta: valores.setdefault(nombre, inicial),
        get=lambda nombre: valores[nombre],
    )
    codigo_entrada = notebook.read_text(encoding="utf-8").split("# MAGIC %run ./00_configuracion")[0]
    contexto = {"dbutils": SimpleNamespace(widgets=widgets)}
    exec(compile(codigo_entrada, str(notebook), "exec"), contexto)
    config = cargar_configuracion(BACI_ENTORNO=contexto["BACI_ENTORNO"])
    assert config["ESQUEMA"] == esquema


@pytest.fixture
def escritura():
    sentencias, vistas, eliminadas = [], [], []

    def sql(sentencia):
        sentencias.append(sentencia)
        return SimpleNamespace(collect=lambda: [])

    spark = SimpleNamespace(sql=sql, catalog=SimpleNamespace(dropTempView=eliminadas.append))
    dataframe = SimpleNamespace(createOrReplaceTempView=vistas.append)
    return spark, dataframe, sentencias, vistas, eliminadas


@pytest.mark.parametrize("entorno,esquema", [("dev", "baci_dev"), ("prod", "baci")])
def test_las_tres_escrituras_solo_usan_el_esquema_elegido(entorno, esquema, escritura):
    spark, dataframe, sentencias, vistas, eliminadas = escritura
    config = cargar_configuracion(BACI_ENTORNO=entorno, spark=spark)
    for clave in ("TABLA_COMERCIO", "TABLA_CONCENTRACION", "TABLA_ALTERNATIVAS"):
        config["guardar_delta"](dataframe, config[clave])
    creaciones = [s for s in sentencias if s.startswith("CREATE OR REPLACE TABLE")]
    assert len(creaciones) == 3
    assert all(s.startswith(f"CREATE OR REPLACE TABLE workspace.{esquema}.") for s in creaciones)
    assert sentencias[0] == f"CREATE SCHEMA IF NOT EXISTS workspace.{esquema}"
    assert vistas == eliminadas


@pytest.mark.parametrize("entorno,tabla", [
    ("dev", "workspace.baci.comercio_enriquecido"),
    ("dev", "workspace.baci_dev.otra_tabla"),
    ("prod", "workspace.baci_dev.comercio_enriquecido"),
])
def test_rechaza_tablas_fuera_del_entorno_sin_crear_vistas_o_esquemas(entorno, tabla, escritura):
    spark, dataframe, sentencias, vistas, eliminadas = escritura
    config = cargar_configuracion(BACI_ENTORNO=entorno, spark=spark)
    with pytest.raises(ValueError, match="fuera de las salidas"):
        config["guardar_delta"](dataframe, tabla)
    assert (sentencias, vistas, eliminadas) == ([], [], [])


@pytest.fixture
def origen(tmp_path):
    config = cargar_configuracion(BACI_ENTORNO="dev")
    config["RUTA_ZIP"] = tmp_path / "origen.zip"
    config["CARPETA_REFERENCE"] = tmp_path / "raw" / "reference"
    config["RUTAS_TRADE"] = [str(tmp_path / "raw" / "trade" / Path(r).name) for r in config["RUTAS_TRADE"]]
    config["RUTA_PAISES"] = str(config["CARPETA_REFERENCE"] / Path(config["RUTA_PAISES"]).name)
    config["RUTA_PRODUCTOS"] = str(config["CARPETA_REFERENCE"] / Path(config["RUTA_PRODUCTOS"]).name)
    rutas = [Path(r) for r in config["RUTAS_TRADE"] + [config["RUTA_PAISES"], config["RUTA_PRODUCTOS"]]]
    rutas.append(config["CARPETA_REFERENCE"] / "Readme.txt")
    with zipfile.ZipFile(config["RUTA_ZIP"], "w") as archivo:
        for ruta in rutas:
            archivo.writestr(ruta.name, b"contenido de prueba\n")
    return config, rutas


def preparar_copias(rutas):
    for ruta in rutas:
        ruta.parent.mkdir(parents=True, exist_ok=True)
        ruta.write_bytes(b"contenido de prueba\n")


def ejecutar_preparacion(config):
    notebook = NOTEBOOKS / "01_archivos_raw.py"
    codigo = notebook.read_text(encoding="utf-8").split("# MAGIC %run ./00_configuracion", 1)[1]
    exec(compile(codigo, str(notebook), "exec"), config)


def test_dev_reutiliza_archivos_sin_modificarlos(origen):
    config, rutas = origen
    preparar_copias(rutas)
    antes = [(p.read_bytes(), p.stat().st_mtime_ns) for p in rutas]
    ejecutar_preparacion(config)
    assert config["conteos"] == {"extraido": 0, "reutilizado": 11}
    assert [(p.read_bytes(), p.stat().st_mtime_ns) for p in rutas] == antes


def test_dev_no_extrae_archivos_faltantes(origen):
    config, rutas = origen
    with pytest.raises(ValueError, match="dev no modifica"):
        ejecutar_preparacion(config)
    assert not rutas[0].parent.parent.exists()


def test_dev_no_repara_un_archivo_de_tamano_distinto(origen):
    config, rutas = origen
    preparar_copias(rutas)
    rutas[0].write_bytes(b"incompleto")
    with pytest.raises(ValueError, match="dev no modifica"):
        ejecutar_preparacion(config)
    assert rutas[0].read_bytes() == b"incompleto"


def test_dev_rechaza_reextraccion_forzada(origen):
    config, rutas = origen
    preparar_copias(rutas)
    config["REEXTRAER"] = True
    with pytest.raises(ValueError, match="REEXTRAER no se permite"):
        ejecutar_preparacion(config)
    assert all(p.read_bytes() == b"contenido de prueba\n" for p in rutas)


def test_prod_conserva_la_extraccion_de_insumos(origen):
    config, rutas = origen
    config["ENTORNO"] = "prod"
    ejecutar_preparacion(config)
    assert config["conteos"] == {"extraido": 11, "reutilizado": 0}
    assert all(p.read_bytes() == b"contenido de prueba\n" for p in rutas)
