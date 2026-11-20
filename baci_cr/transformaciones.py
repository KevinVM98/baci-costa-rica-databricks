"""Transformaciones PySpark compartidas por los notebooks y sus pruebas.

Reciben DataFrames y parámetros explícitos. La lectura, validación de entradas
y escritura de tablas siguen siendo responsabilidad de los notebooks.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F


def enriquecer_comercio(df_comercio, df_paises, df_productos, clasificacion, version_baci):
    """Unir catálogos validados y convertir miles de USD; los códigos llegan normalizados."""
    df_exportadores = df_paises.select(
        F.col("country_code").alias("i"), F.col("country_name").alias("exportador")
    )
    df_importadores = df_paises.select(
        F.col("country_code").alias("j"), F.col("country_name").alias("importador")
    )
    df_descripciones = df_productos.select(
        F.col("code").alias("k"), F.col("description").alias("producto")
    )

    df_enriquecido = (
        df_comercio
        .join(F.broadcast(df_exportadores), "i", "left")
        .join(F.broadcast(df_importadores), "j", "left")
        .join(F.broadcast(df_descripciones), "k", "left")
        .select(
            F.col("t").alias("anio"),
            F.col("i").alias("codigo_exportador"), "exportador",
            F.col("j").alias("codigo_importador"), "importador",
            F.col("k").alias("codigo_producto"), "producto",
            F.col("v").alias("valor_miles_usd"),
            (F.col("v") * F.lit(1000)).alias("valor_usd"),
            F.col("q").alias("toneladas"),
            F.lit(clasificacion).alias("clasificacion"),
            F.lit(version_baci).alias("version_baci")
        )
    )
    return df_enriquecido


def calcular_concentracion(df_global, claves, codigo_cr):
    """Calcular el principal proveedor por producto, año, clasificación y edición."""
    df_por_pais = (
        df_global.filter(F.col("codigo_importador") == codigo_cr)
        .groupBy(*claves, "producto", "codigo_exportador", "exportador")
        .agg(F.sum("valor_usd").alias("valor_pais_usd"))
    )

    ventana_producto = Window.partitionBy(*claves)
    orden_principal = ventana_producto.orderBy(F.desc("valor_pais_usd"), F.asc("codigo_exportador"))

    df_concentracion = (
        df_por_pais
        .withColumn("total_importado_usd", F.sum("valor_pais_usd").over(ventana_producto))
        .withColumn("paises_exportadores", F.count("*").over(ventana_producto))
        .withColumn("posicion", F.row_number().over(orden_principal))
        .filter(F.col("posicion") == 1)
        .withColumn(
            "participacion_principal_pct",
            F.when(F.col("total_importado_usd") > 0,
                   F.lit(100) * F.col("valor_pais_usd") / F.col("total_importado_usd"))
        )
        .select(
            *claves, "producto",
            F.col("codigo_exportador").alias("codigo_exportador_principal"),
            F.col("exportador").alias("exportador_principal"),
            F.col("valor_pais_usd").alias("valor_principal_usd"),
            "total_importado_usd", "paises_exportadores", "participacion_principal_pct"
        )
    )
    return df_concentracion


def seleccionar_productos(df_concentracion, umbral_pct, minimo_importaciones_usd):
    """Aplicar ambos umbrales inclusivos sobre los valores sin redondear."""
    return df_concentracion.filter(
        (F.col("participacion_principal_pct") >= umbral_pct)
        & (F.col("total_importado_usd") >= minimo_importaciones_usd)
    )


def calcular_alternativas(df_global, df_objetivos, claves, codigo_cr, top_alternativas):
    """Ordenar candidatos por valor, destinos y código, excluyendo CR y al principal."""
    df_exportaciones = (
        df_global
        .join(F.broadcast(df_objetivos.select(*claves)), on=claves, how="left_semi")
        .groupBy(*claves, "codigo_exportador", "exportador")
        .agg(
            F.sum("valor_usd").alias("exportaciones_mundiales_usd"),
            F.countDistinct("codigo_importador").alias("destinos"),
            F.sum(F.when(F.col("codigo_importador") == codigo_cr, F.col("valor_usd")).otherwise(0))
            .alias("exportaciones_a_cr_usd")
        )
    )
    orden_alternativas = Window.partitionBy(*claves).orderBy(
        F.desc("exportaciones_mundiales_usd"), F.desc("destinos"), F.asc("codigo_exportador")
    )
    df_alternativas = (
        df_exportaciones
        .join(F.broadcast(df_objetivos), on=claves, how="inner")
        .filter(
            (F.col("codigo_exportador") != codigo_cr)
            & (F.col("codigo_exportador") != F.col("codigo_exportador_principal"))
            & (F.col("exportaciones_mundiales_usd") > 0)
        )
        .withColumn("posicion_alternativa", F.row_number().over(orden_alternativas))
        .filter(F.col("posicion_alternativa") <= top_alternativas)
        .withColumnRenamed("codigo_exportador", "codigo_exportador_alternativo")
        .withColumnRenamed("exportador", "exportador_alternativo")
    )
    return df_alternativas
