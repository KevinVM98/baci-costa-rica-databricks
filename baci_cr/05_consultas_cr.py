# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Consultas del proyecto
# MAGIC Consulta las tablas analíticas guardadas. No reconstruye tablas ni vuelve a extraer archivos.
# MAGIC
# MAGIC Cambiá `ANIO_CONSULTA` y `PRODUCTO_CONSULTA` en la celda siguiente para explorar otro caso. Los montos son valores nominales de comercio en USD, no volumen físico. Los porcentajes se redondean únicamente al mostrar resultados.
# MAGIC
# MAGIC El resumen anual suma la tabla de concentración, que tiene una fila por producto y año. Así evita multiplicar las importaciones por la cantidad de alternativas.

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

from pyspark.sql import functions as F

ANIO_CONSULTA = 2023
PRODUCTO_CONSULTA = "230330"

df_indicadores = spark.table(TABLA_CONCENTRACION)
df_alternativas = spark.table(TABLA_ALTERNATIVAS)

seleccionado = (
    (F.col("participacion_principal_pct") >= UMBRAL_CONCENTRACION_PCT)
    & (F.col("total_importado_usd") >= MINIMO_IMPORTACIONES_USD)
)
df_resumen_anual = (
    df_indicadores.groupBy("anio", "clasificacion", "version_baci")
    .agg(
        F.count("*").alias("productos_importados"),
        F.sum("total_importado_usd").alias("importaciones_totales_usd"),
        F.sum(F.when(seleccionado, 1).otherwise(0)).alias("productos_seleccionados"),
        F.sum(F.when(seleccionado, F.col("total_importado_usd")).otherwise(0))
        .alias("importaciones_seleccionadas_usd")
    )
    .withColumn(
        "participacion_valor_seleccionado_pct",
        F.when(F.col("importaciones_totales_usd") > 0,
               F.round(F.lit(100) * F.col("importaciones_seleccionadas_usd") / F.col("importaciones_totales_usd"), 2))
    )
)
display(df_resumen_anual.orderBy("anio"))

# COMMAND ----------

# Casos seleccionados del año, ordenados por el importe importado por CR.
display(
    df_indicadores.filter((F.col("anio") == ANIO_CONSULTA) & seleccionado)
    .orderBy(F.desc("total_importado_usd"), F.asc("codigo_producto"))
    .select(
        "anio", "codigo_producto", "producto", "exportador_principal",
        F.round("participacion_principal_pct", 6).alias("participacion_principal_pct"),
        "total_importado_usd", "paises_exportadores"
    ).limit(20)
)

# COMMAND ----------

# Evolución anual del producto elegido.
display(
    df_indicadores.filter(F.col("codigo_producto") == PRODUCTO_CONSULTA)
    .orderBy("anio")
    .select(
        "anio", "exportador_principal",
        F.round("participacion_principal_pct", 6).alias("participacion_principal_pct"),
        "paises_exportadores", "total_importado_usd"
    )
)

# COMMAND ----------

# Países candidatos disponibles en el ranking seleccionado.
display(
    df_alternativas.filter(
        (F.col("anio") == ANIO_CONSULTA)
        & (F.col("codigo_producto") == PRODUCTO_CONSULTA)
    )
    .orderBy("posicion_alternativa")
    .select(
        "anio", "codigo_producto", "exportador_principal",
        "posicion_alternativa", "exportador_alternativo",
        "exportaciones_mundiales_usd", "destinos", "exportaciones_a_cr_usd"
    )
)

