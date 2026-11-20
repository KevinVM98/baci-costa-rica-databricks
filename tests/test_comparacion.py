"""Pruebas del control de igualdad entre entornos con datos sintéticos."""

import pytest

from baci_cr.comparacion import comparar_contenido


ESQUEMA = "anio INT, codigo_producto STRING, valor INT"


def crear(spark, filas, esquema=ESQUEMA):
    return spark.createDataFrame(filas, schema=esquema)


def test_igualdad_independiente_del_orden_de_filas_y_columnas(spark):
    dev = crear(spark, [(2022, "000001", 100), (2023, "000002", 200)])
    prod = crear(spark, [(2023, "000002", 200), (2022, "000001", 100)])
    resultado = comparar_contenido(dev, prod.select("valor", "codigo_producto", "anio"))
    assert resultado["iguales"]
    assert resultado["filas_solo_dev"] == 0
    assert resultado["filas_solo_prod"] == 0
    assert resultado["ejemplos_solo_dev"] == resultado["ejemplos_solo_prod"] == []


def test_detecta_valores_distintos_aunque_los_conteos_coincidan(spark):
    dev = crear(spark, [(2023, "230330", 100)])
    prod = crear(spark, [(2023, "230330", 101)])
    resultado = comparar_contenido(dev, prod)
    assert not resultado["iguales"]
    assert (resultado["filas_solo_dev"], resultado["filas_solo_prod"]) == (1, 1)
    assert resultado["ejemplos_solo_dev"][0]["valor"] == 100
    assert resultado["ejemplos_solo_prod"][0]["valor"] == 101


def test_detecta_multiplicidad_aunque_conserve_el_mismo_registro(spark):
    dev = crear(spark, [(2023, "230330", 100), (2023, "230330", 100)])
    prod = crear(spark, [(2023, "230330", 100)])
    resultado = comparar_contenido(dev, prod)
    assert (resultado["filas_solo_dev"], resultado["filas_solo_prod"]) == (1, 0)
    assert not resultado["iguales"]


def test_nulos_iguales_y_vacios(spark):
    filas = [(2023, "230330", None)]
    assert comparar_contenido(crear(spark, filas), crear(spark, filas))["iguales"]
    assert comparar_contenido(crear(spark, []), crear(spark, []))["iguales"]


def test_rechaza_diferencias_de_columnas_y_tipos(spark):
    dev = crear(spark, [(2023, "230330", 100)])
    prod_sin_columna = dev.drop("valor")
    with pytest.raises(ValueError, match="Esquemas incompatibles"):
        comparar_contenido(dev, prod_sin_columna)

    prod_otro_tipo = crear(
        spark, [(2023, "230330", 100.0)],
        "anio INT, codigo_producto STRING, valor DOUBLE",
    )
    with pytest.raises(ValueError, match="Tipos incompatibles"):
        comparar_contenido(dev, prod_otro_tipo)


@pytest.mark.parametrize("limite", [-1, 1.5, "3"])
def test_rechaza_limites_invalidos(spark, limite):
    dev = crear(spark, [(2023, "230330", 100)])
    with pytest.raises(ValueError, match="max_ejemplos"):
        comparar_contenido(dev, dev, max_ejemplos=limite)


def test_cero_ejemplos_conserva_los_conteos(spark):
    dev = crear(spark, [(2023, "230330", 100)])
    prod = crear(spark, [(2023, "230330", 101)])
    resultado = comparar_contenido(dev, prod, max_ejemplos=0)
    assert not resultado["iguales"]
    assert resultado["filas_solo_dev"] == resultado["filas_solo_prod"] == 1
    assert resultado["ejemplos_solo_dev"] == resultado["ejemplos_solo_prod"] == []
