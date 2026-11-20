"""Reglas de 04 y 07 contra resultados conocidos, sin datos del workspace."""

import pytest
from pyspark.sql import functions as F

from calidad import evaluar_calidad


PARAMETROS = {
    "claves": ["anio", "codigo_producto", "clasificacion", "version_baci"],
    "anios": [2023], "clasificacion": "HS92", "version_baci": "prueba",
    "top_alternativas": 3, "codigo_cr": 188,
    "umbral_concentracion_pct": 90, "minimo_importaciones_usd": 1000000,
}


@pytest.fixture
def tablas_calidad(spark):
    indicadores = spark.createDataFrame(
        [(2023, "000001", "HS92", "prueba", 100, 90.0, 1000000.0)],
        "anio INT, codigo_producto STRING, clasificacion STRING, version_baci STRING, "
        "codigo_exportador_principal INT, participacion_principal_pct DOUBLE, "
        "total_importado_usd DOUBLE",
    )
    alternativas = spark.createDataFrame(
        [(2023, "000001", "HS92", "prueba", 100, 200, 1, 2000000.0, 0.0, 2),
         (2023, "000001", "HS92", "prueba", 100, 300, 2, 1000000.0, 100000.0, 1)],
        "anio INT, codigo_producto STRING, clasificacion STRING, version_baci STRING, "
        "codigo_exportador_principal INT, codigo_exportador_alternativo INT, "
        "posicion_alternativa INT, exportaciones_mundiales_usd DOUBLE, "
        "exportaciones_a_cr_usd DOUBLE, destinos INT",
    )
    return indicadores, alternativas


def evaluar(indicadores, alternativas):
    return evaluar_calidad(indicadores, alternativas, **PARAMETROS)


def test_tablas_validas_incluyen_limites_exactos_y_metricas(tablas_calidad):
    resultado = evaluar(*tablas_calidad)
    assert len(resultado["errores"]) == 6
    assert all(cantidad == 0 for cantidad in resultado["errores"].values())
    assert resultado["metricas"] == {
        "filas_indicadores": 1, "productos_en_el_periodo": 1, "anios": 1,
        "anio_minimo": 2023, "anio_maximo": 2023,
        "filas_alternativas": 2, "casos_sin_candidato": 0,
    }


def test_indicadores_vacios_no_aprueban_por_ausencia_de_filas(tablas_calidad):
    indicadores, alternativas = tablas_calidad
    resultado = evaluar(indicadores.limit(0), alternativas.limit(0))
    assert resultado["errores"]["indicadores_vacios"] == 1
    assert resultado["errores"]["cobertura_incorrecta"] == 1
    assert resultado["metricas"]["anio_minimo"] is None
    assert resultado["metricas"]["filas_indicadores"] == 0


@pytest.mark.parametrize("columna,valor", [
    ("anio", 2022), ("clasificacion", "OTRA"),
    ("version_baci", "otra_edicion"), ("version_baci", None),
])
def test_cobertura_detecta_periodo_y_edicion(columna, valor, tablas_calidad):
    indicadores, alternativas = tablas_calidad
    resultado = evaluar(indicadores.withColumn(columna, F.lit(valor)), alternativas)
    assert resultado["errores"]["cobertura_incorrecta"] == 1


def test_duplicados_y_rankings_cuentan_grupos(tablas_calidad):
    indicadores, alternativas = tablas_calidad
    resultado = evaluar(indicadores.unionByName(indicadores), alternativas.unionByName(alternativas))
    assert resultado["errores"]["indicadores_duplicados"] == 1
    assert resultado["errores"]["rankings_invalidos"] == 1
    assert resultado["metricas"]["filas_indicadores"] == 2
    assert resultado["metricas"]["filas_alternativas"] == 4


def test_ranking_rechaza_huecos_y_origen_distinto_de_uno(tablas_calidad):
    indicadores, alternativas = tablas_calidad
    alteradas = alternativas.withColumn("posicion_alternativa", F.col("posicion_alternativa") + 1)
    assert evaluar(indicadores, alteradas)["errores"]["rankings_invalidos"] == 1


def test_paises_y_montos_invalidos_cuentan_filas(tablas_calidad):
    indicadores, alternativas = tablas_calidad
    base = alternativas.filter("posicion_alternativa = 1")
    cambios = [
        ("codigo_exportador_alternativo", 188),
        ("codigo_exportador_alternativo", 100),
        ("exportaciones_mundiales_usd", 0.0),
        ("exportaciones_mundiales_usd", None),
        ("exportaciones_a_cr_usd", -1.0),
        ("exportaciones_a_cr_usd", 2000001.0),
        ("exportaciones_a_cr_usd", None),
        ("destinos", 0), ("destinos", None),
    ]
    malas = base.limit(0)
    for columna, valor in cambios:
        malas = malas.unionByName(base.withColumn(columna, F.lit(valor)))
    resultado = evaluar(indicadores, malas)
    assert resultado["errores"]["filas_con_paises_o_valores_invalidos"] == 9


def test_alternativas_de_otro_principal_o_fuera_del_umbral(tablas_calidad):
    indicadores, alternativas = tablas_calidad
    indicadores = indicadores.withColumn("participacion_principal_pct", F.lit(89.999))
    resultado = evaluar(indicadores, alternativas)
    assert resultado["errores"]["alternativas_sin_caso_valido"] == 2


def test_sin_candidatos_es_informativo_y_no_inventa_errores(tablas_calidad):
    indicadores, alternativas = tablas_calidad
    resultado = evaluar(indicadores, alternativas.limit(0))
    assert all(cantidad == 0 for cantidad in resultado["errores"].values())
    assert resultado["metricas"]["casos_sin_candidato"] == 1
