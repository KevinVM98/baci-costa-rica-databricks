"""Ejemplos sintéticos con resultados esperados calculados a mano."""

from decimal import Decimal

import pytest

from transformaciones import (
    calcular_alternativas,
    calcular_concentracion,
    enriquecer_comercio,
    seleccionar_productos,
)


CLAVES = ["anio", "codigo_producto", "clasificacion", "version_baci"]
CODIGO_CR = 188


def test_enriquece_sin_perder_filas_y_convierte_miles_usd_una_vez(spark):
    comercio = spark.createDataFrame(
        [
            (2023, 840, 188, "010511", Decimal("1234.567"), None),
            (2023, 188, 840, "230330", Decimal("2.001"), 2.0),
            (2022, 792, 188, "720824", Decimal("0.001"), 0.0),
        ],
        "t INT, i INT, j INT, k STRING, v DECIMAL(20,3), q DOUBLE",
    )
    paises = spark.createDataFrame(
        [(840, "USA"), (188, "Costa Rica"), (792, "Türkiye")],
        "country_code INT, country_name STRING",
    )
    productos = spark.createDataFrame(
        [("010511", "Producto A"), ("230330", "Producto B"), ("720824", "Producto C")],
        "code STRING, description STRING",
    )
    resultado = enriquecer_comercio(comercio, paises, productos, "HS92", "202501")
    registros = resultado.collect()
    assert len(registros) == 3
    filas = {fila.codigo_producto: fila for fila in registros}
    assert len(filas) == 3
    assert filas["010511"].valor_miles_usd == Decimal("1234.567")
    assert filas["010511"].valor_usd == Decimal("1234567")
    assert filas["230330"].valor_usd == Decimal("2001")
    assert filas["720824"].valor_usd == Decimal("1")
    assert filas["010511"].toneladas is None
    assert filas["720824"].toneladas == 0.0
    assert filas["010511"].exportador == "USA"
    assert filas["010511"].importador == "Costa Rica"
    assert filas["230330"].exportador == "Costa Rica"
    assert filas["230330"].importador == "USA"
    assert filas["720824"].exportador == "Türkiye"
    assert filas["010511"].producto == "Producto A"
    assert all(f.clasificacion == "HS92" and f.version_baci == "202501" for f in filas.values())


def test_concentracion_agrega_flujos_y_separa_producto_anio_y_edicion(crear_comercio):
    comercio = crear_comercio([
        (2023, "230330", 840, 188, "600000", "202501"),
        (2023, "230330", 840, 188, "350000", "202501"),
        (2023, "230330", 156, 188, "50000", "202501"),
        # Otro importador no debe aumentar las importaciones de CR.
        (2023, "230330", 840, 484, "9000000", "202501"),
        (2022, "230330", 840, 188, "800000", "202501"),
        (2022, "230330", 156, 188, "200000", "202501"),
        (2023, "010511", 840, 188, "1000000", "202501"),
        # Edición ficticia para comprobar la separación por clave.
        (2023, "230330", 840, 188, "850000", "prueba_otra_edicion"),
        (2023, "230330", 156, 188, "150000", "prueba_otra_edicion"),
    ])
    filas = calcular_concentracion(comercio, CLAVES, CODIGO_CR).collect()
    por_clave = {(f.anio, f.codigo_producto, f.version_baci): f for f in filas}
    assert len(filas) == 4
    principal = por_clave[(2023, "230330", "202501")]
    assert principal.total_importado_usd == Decimal("1000000")
    assert principal.valor_principal_usd == Decimal("950000")
    assert principal.participacion_principal_pct == Decimal("95")
    assert principal.paises_exportadores == 2
    assert principal.codigo_exportador_principal == 840
    assert por_clave[(2022, "230330", "202501")].participacion_principal_pct == Decimal("80")
    assert por_clave[(2023, "010511", "202501")].participacion_principal_pct == Decimal("100")
    assert por_clave[(2023, "230330", "prueba_otra_edicion")].participacion_principal_pct == Decimal("85")


def test_empate_del_principal_elige_el_menor_codigo(crear_comercio):
    comercio = crear_comercio([
        (2023, "230330", 840, 188, "500000", "202501"),
        (2023, "230330", 156, 188, "500000", "202501"),
    ])
    fila = calcular_concentracion(comercio, CLAVES, CODIGO_CR).first()
    assert fila.codigo_exportador_principal == 156
    assert fila.participacion_principal_pct == Decimal("50")


def test_total_cero_produce_porcentaje_nulo(crear_comercio):
    comercio = crear_comercio([(2023, "230330", 840, 188, "0", "202501")])
    fila = calcular_concentracion(comercio, CLAVES, CODIGO_CR).first()
    assert fila.total_importado_usd == Decimal("0")
    assert fila.participacion_principal_pct is None


def test_sin_importaciones_a_cr_no_hay_indicadores(crear_comercio):
    comercio = crear_comercio([(2023, "230330", 840, 484, "1000000", "202501")])
    assert calcular_concentracion(comercio, CLAVES, CODIGO_CR).count() == 0


def test_umbrales_inclusivos_sin_redondear_y_con_nulos(spark):
    indicadores = spark.createDataFrame(
        [
            ("supera", Decimal("95"), Decimal("2000000")),
            ("limite_exacto", Decimal("90"), Decimal("1000000")),
            ("baja_concentracion", Decimal("85"), Decimal("2000000")),
            ("casi_90", Decimal("89.999999"), Decimal("2000000")),
            ("monto_insuficiente", Decimal("95"), Decimal("999999.999")),
            ("porcentaje_nulo", None, Decimal("2000000")),
            ("monto_nulo", Decimal("95"), None),
        ],
        "caso STRING, participacion_principal_pct DECIMAL(18,9), total_importado_usd DECIMAL(24,3)",
    )
    filas = seleccionar_productos(indicadores, 90, 1000000).collect()
    assert {f.caso for f in filas} == {"supera", "limite_exacto"}
    mas_exigentes = seleccionar_productos(indicadores, 95, 2000000).collect()
    assert {f.caso for f in mas_exigentes} == {"supera"}


@pytest.fixture
def escenario_ranking(crear_comercio):
    comercio = crear_comercio([
        (2023, "230330", 840, 188, "950000", "202501"),
        (2023, "230330", 156, 188, "50000", "202501"),
        # Principal y CR se excluyen aunque tengan grandes exportaciones.
        (2023, "230330", 840, 484, "9000000", "202501"),
        (2023, "230330", 188, 484, "8000000", "202501"),
        (2023, "230330", 528, 484, "300000", "202501"),
        (2023, "230330", 528, 724, "200000", "202501"),
        (2023, "230330", 76, 484, "500000", "202501"),
        (2023, "230330", 348, 484, "250000", "202501"),
        (2023, "230330", 348, 724, "250000", "202501"),
        (2023, "230330", 250, 484, "0", "202501"),
        (2023, "230330", 32, 484, "-5", "202501"),
        # Flujos ajenos al producto-año-edición seleccionado.
        (2023, "010511", 528, 484, "9000000", "202501"),
        (2022, "230330", 528, 484, "9000000", "202501"),
        (2023, "230330", 528, 484, "9000000", "prueba_otra_edicion"),
    ])
    indicadores = calcular_concentracion(comercio, CLAVES, CODIGO_CR)
    objetivos = seleccionar_productos(indicadores, 90, 1000000)
    return comercio, objetivos


@pytest.mark.parametrize(
    "limite, esperados",
    [(1, [348]), (2, [348, 528]), (3, [348, 528, 76]), (5, [348, 528, 76, 156])],
)
def test_ranking_excluye_paises_y_desempata_por_destinos_y_codigo(escenario_ranking, limite, esperados):
    comercio, objetivos = escenario_ranking
    filas = calcular_alternativas(comercio, objetivos, CLAVES, CODIGO_CR, limite).orderBy(
        "posicion_alternativa"
    ).collect()
    assert [f.codigo_exportador_alternativo for f in filas] == esperados
    assert [f.posicion_alternativa for f in filas] == list(range(1, len(esperados) + 1))
    assert all(f.codigo_exportador_principal == 840 for f in filas)
    assert all(f.anio == 2023 and f.codigo_producto == "230330" and f.version_baci == "202501" for f in filas)
    assert [f.destinos for f in filas] == [2, 2, 1, 1][:len(esperados)]
    assert [f.exportaciones_mundiales_usd for f in filas] == [Decimal("500000"), Decimal("500000"), Decimal("500000"), Decimal("50000")][:len(esperados)]
    assert [f.exportaciones_a_cr_usd for f in filas] == [Decimal("0"), Decimal("0"), Decimal("0"), Decimal("50000")][:len(esperados)]


def test_alternativa_puede_tener_ventas_previas_a_cr(crear_comercio):
    comercio = crear_comercio([
        (2023, "230330", 840, 188, "900000", "202501"),
        (2023, "230330", 156, 188, "100000", "202501"),
        (2023, "230330", 156, 484, "250000", "202501"),
    ])
    objetivos = seleccionar_productos(calcular_concentracion(comercio, CLAVES, CODIGO_CR), 90, 1000000)
    filas = calcular_alternativas(comercio, objetivos, CLAVES, CODIGO_CR, 3).collect()
    assert len(filas) == 1
    assert filas[0].codigo_exportador_alternativo == 156
    assert filas[0].exportaciones_mundiales_usd == Decimal("350000")
    assert filas[0].exportaciones_a_cr_usd == Decimal("100000")
    assert filas[0].destinos == 2
    assert filas[0].posicion_alternativa == 1


def test_sin_candidatos_no_se_inventan_alternativas(crear_comercio):
    comercio = crear_comercio([
        (2023, "230330", 840, 188, "1000000", "202501"),
        (2023, "230330", 188, 484, "9000000", "202501"),
    ])
    objetivos = seleccionar_productos(calcular_concentracion(comercio, CLAVES, CODIGO_CR), 90, 1000000)
    assert calcular_alternativas(comercio, objetivos, CLAVES, CODIGO_CR, 3).count() == 0
