"""Spark local y datos sintéticos: no se accede al workspace ni a los CSV BACI."""

import os
import sys
from decimal import Decimal

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    previous_python = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = None
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("baci-unit-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        if session is not None:
            session.stop()
        if previous_python is None:
            os.environ.pop("PYSPARK_PYTHON", None)
        else:
            os.environ["PYSPARK_PYTHON"] = previous_python


@pytest.fixture
def crear_comercio(spark):
    """Crear flujos ficticios: año, producto, exportador, importador, USD y edición."""
    def crear(filas):
        datos = [
            (
                anio, producto, exportador, f"País de prueba {exportador}",
                importador, Decimal(str(monto)), f"Producto de prueba {producto}",
                "HS92", version,
            )
            for anio, producto, exportador, importador, monto, version in filas
        ]
        return spark.createDataFrame(
            datos,
            "anio INT, codigo_producto STRING, codigo_exportador INT, "
            "exportador STRING, codigo_importador INT, valor_usd DECIMAL(24,3), "
            "producto STRING, clasificacion STRING, version_baci STRING",
        )
    return crear
