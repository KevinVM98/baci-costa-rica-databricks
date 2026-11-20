"""Contrato de persistencia, versiones y fallas, sin escribir en Databricks."""

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

import monitoreo


class SparkNoPermitido:
    def __getattr__(self, nombre):
        raise AssertionError("La guarda debe fallar antes de usar Spark")


class SparkHistorial:
    """Frontera simulada de Delta: registra versiones leídas y lotes escritos."""
    def __init__(self):
        self.sentencias = []
        self.lecturas = []
        self.lotes = []
        self.operaciones_escritura = []
        self.fallo_lectura = False
        self.fallo_escritura = False

    def sql(self, sentencia):
        self.sentencias.append(sentencia)
        if sentencia.startswith("DESCRIBE HISTORY"):
            if self.fallo_lectura:
                raise LookupError("mensaje original de lectura")
            version = 10 if "concentracion" in sentencia else 20
            return SimpleNamespace(first=lambda: {"version": version})
        assert sentencia == "CREATE SCHEMA IF NOT EXISTS workspace.baci_dev"
        return SimpleNamespace(collect=lambda: [])

    @property
    def read(self):
        def option(nombre, version):
            assert nombre == "versionAsOf"
            def table(tabla):
                self.lecturas.append((tabla, version))
                return (tabla, version)
            return SimpleNamespace(table=table)
        return SimpleNamespace(option=option)

    def createDataFrame(self, filas, schema):
        assert schema == monitoreo.ESQUEMA_HISTORIAL
        padre = self
        class Writer:
            def format(self, valor):
                padre.operaciones_escritura.append(("format", valor))
                return self

            def mode(self, valor):
                padre.operaciones_escritura.append(("mode", valor))
                return self

            def saveAsTable(self, tabla):
                padre.operaciones_escritura.append(("table", tabla))
                if padre.fallo_escritura:
                    raise OSError("no se puede persistir")
                padre.lotes.append(deepcopy(filas))
        return SimpleNamespace(write=Writer())


@pytest.fixture
def frontera(monkeypatch):
    spark = SparkHistorial()
    resultado = {"errores": {"regla_de_prueba": 0}, "metricas": {"filas_indicadores": 1}}
    monkeypatch.setattr(monitoreo, "evaluar_calidad", lambda *args, **kwargs: resultado)
    return spark, resultado


def test_conserva_versiones_contexto_y_usa_append(frontera):
    spark, _ = frontera
    parametros = {"anios": [2023], "version_baci": "prueba"}
    filas = monitoreo.ejecutar_monitoreo(
        spark, parametros, job_id="101", job_run_id="202", task_run_id="303", intento="1"
    )
    assert spark.lecturas == [(monitoreo.TABLA_INDICADORES, 10), (monitoreo.TABLA_ALTERNATIVAS, 20)]
    assert spark.lotes == [filas]
    assert spark.operaciones_escritura == [
        ("format", "delta"), ("mode", "append"), ("table", monitoreo.TABLA_HISTORIAL)
    ]
    for fila in filas:
        assert fila["version_indicadores"] == 10 and fila["version_alternativas"] == 20
        assert fila["job_run_id"] == "202" and fila["task_run_id"] == "303"
        assert fila["intento"] == 1 and fila["estado_revision"] == "APROBADO"
        assert json.loads(fila["parametros_json"]) == parametros
        assert fila["fecha_utc"].utcoffset().total_seconds() == 0
    assert len({fila["id_revision"] for fila in filas}) == 1


def test_guarda_reglas_fallidas_antes_de_marcar_error(frontera):
    spark, resultado = frontera
    resultado["errores"]["regla_de_prueba"] = 2
    with pytest.raises(ValueError, match="Calidad fallida; diagnóstico guardado"):
        monitoreo.ejecutar_monitoreo(spark, {})
    assert len(spark.lotes) == 1
    regla, metrica = spark.lotes[0]
    assert regla["valor"] == 2 and regla["estado"] == "FALLIDO"
    assert metrica["estado"] == "INFORMATIVO" and metrica["estado_revision"] == "FALLIDO"


def test_error_de_lectura_se_registra_y_conserva_excepcion(frontera):
    spark, _ = frontera
    spark.fallo_lectura = True
    with pytest.raises(LookupError, match="mensaje original de lectura"):
        monitoreo.ejecutar_monitoreo(spark, {})
    fila, = spark.lotes[0]
    assert fila["tipo"] == "ERROR_TECNICO" and fila["estado_revision"] == "ERROR"
    assert fila["nombre"] == "leer_versiones" and fila["detalle"] == "LookupError"
    assert fila["version_indicadores"] is None and fila["version_alternativas"] is None


def test_error_al_evaluar_preserva_versiones_y_no_aprueba(frontera, monkeypatch):
    spark, _ = frontera
    def fallar(*args, **kwargs):
        raise RuntimeError("dato privado de ejemplo")
    monkeypatch.setattr(monitoreo, "evaluar_calidad", fallar)
    with pytest.raises(RuntimeError, match="dato privado de ejemplo"):
        monitoreo.ejecutar_monitoreo(spark, {})
    fila, = spark.lotes[0]
    assert fila["nombre"] == "evaluar_reglas" and fila["detalle"] == "RuntimeError"
    assert fila["version_indicadores"] == 10 and fila["version_alternativas"] == 20
    assert "dato privado" not in str(fila)


def test_fallo_de_persistencia_impide_resultado_exitoso(frontera):
    spark, _ = frontera
    spark.fallo_escritura = True
    with pytest.raises(OSError, match="no se puede persistir"):
        monitoreo.ejecutar_monitoreo(spark, {})
    assert spark.lotes == []


def test_reintentos_conservan_historial_con_nueva_revision(frontera):
    spark, _ = frontera
    primera = monitoreo.ejecutar_monitoreo(spark, {}, job_run_id="202", intento="1")
    segunda = monitoreo.ejecutar_monitoreo(spark, {}, job_run_id="202", intento="2")
    assert len(spark.lotes) == 2
    assert spark.lotes[0] == primera and spark.lotes[1] == segunda
    assert primera[0]["id_revision"] != segunda[0]["id_revision"]
    assert primera[0]["job_run_id"] == segunda[0]["job_run_id"]


def test_registros_manuales_y_errores_tienen_esquema_spark_valido(frontera, spark):
    simulado, _ = frontera
    buenas = monitoreo.ejecutar_monitoreo(simulado, {})
    simulado.fallo_lectura = True
    with pytest.raises(LookupError):
        monitoreo.ejecutar_monitoreo(simulado, {})
    filas = buenas + simulado.lotes[-1]
    registros = spark.createDataFrame(filas, schema=monitoreo.ESQUEMA_HISTORIAL).collect()
    assert len(registros) == 3
    assert all(f.job_id is None and f.job_run_id is None and f.intento is None for f in registros)
    assert registros[-1].version_indicadores is None and registros[-1].valor is None


@pytest.mark.parametrize("entorno", ["prod", "", "DEV"])
def test_entorno_invalido_no_lee_ni_escribe(entorno):
    with pytest.raises(ValueError, match="Monitoreo bloqueado"):
        monitoreo.ejecutar_monitoreo(SparkNoPermitido(), {}, entorno=entorno)
    with pytest.raises(ValueError, match="Monitoreo bloqueado"):
        monitoreo.guardar_historial(SparkNoPermitido(), [], entorno)


@pytest.mark.parametrize("constante", ["TABLA_HISTORIAL", "TABLA_INDICADORES", "TABLA_ALTERNATIVAS"])
def test_mutar_destinos_a_prod_falla_antes_de_spark(constante, monkeypatch):
    monkeypatch.setattr(monitoreo, constante, "workspace.baci.otra_tabla")
    with pytest.raises(ValueError, match="Monitoreo bloqueado"):
        monitoreo.ejecutar_monitoreo(SparkNoPermitido(), {})
    with pytest.raises(ValueError, match="Monitoreo bloqueado"):
        monitoreo.guardar_historial(SparkNoPermitido(), [], "dev")


def test_lector_rechaza_tablas_fuera_del_alcance():
    with pytest.raises(ValueError, match="lectura no permitida"):
        monitoreo.obtener_version(SparkNoPermitido(), "workspace.baci_dev.comercio_enriquecido")


@pytest.mark.parametrize("contexto", [
    {"job_run_id": "{{job.run_id}}"}, {"intento": "-1"}, {"task_run_id": "texto"},
])
def test_rechaza_metadatos_invalidos_antes_de_spark(contexto):
    with pytest.raises(ValueError, match="Metadato"):
        monitoreo.ejecutar_monitoreo(SparkNoPermitido(), {}, **contexto)


def test_historial_no_admite_lotes_vacios_o_entorno_falso():
    with pytest.raises(ValueError, match="sin resultados"):
        monitoreo.guardar_historial(SparkNoPermitido(), [], "dev")
    with pytest.raises(ValueError, match="pertenecer a dev"):
        monitoreo.guardar_historial(SparkNoPermitido(), [{"entorno": "prod"}], "dev")
