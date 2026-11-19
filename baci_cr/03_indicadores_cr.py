# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Concentración y países alternativos
# MAGIC Lee la tabla global guardada. Primero calcula un indicador por producto y año para las importaciones de Costa Rica. Después busca hasta tres exportadores alternativos para los casos seleccionados.
# MAGIC
# MAGIC La participación se guarda sin redondearla para presentación. Los nombres de países no se usan como claves. En la tabla de alternativas, el total importado por CR se repite por cada candidato: para sumar importaciones por año usá la tabla de concentración.
# MAGIC
# MAGIC Esta etapa reemplaza ambas tablas analíticas. Los candidatos describen comercio histórico, no empresas proveedoras ni disponibilidad comercial actual.

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

df_global = spark.table(TABLA_COMERCIO).filter(
    F.col("anio").between(ANIO_INICIAL, ANIO_FINAL)
    & (F.col("clasificacion") == CLASIFICACION)
    & (F.col("version_baci") == VERSION_BACI)
)
df_por_pais = (
    df_global.filter(F.col("codigo_importador") == CODIGO_CR)
    .groupBy(*CLAVES, "producto", "codigo_exportador", "exportador")
    .agg(F.sum("valor_usd").alias("valor_pais_usd"))
)

ventana_producto = Window.partitionBy(*CLAVES)
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
        *CLAVES, "producto",
        F.col("codigo_exportador").alias("codigo_exportador_principal"),
        F.col("exportador").alias("exportador_principal"),
        F.col("valor_pais_usd").alias("valor_principal_usd"),
        "total_importado_usd", "paises_exportadores", "participacion_principal_pct"
    )
)

if not df_concentracion.limit(1).count():
    raise ValueError("No hay indicadores para la configuración: revisá la etapa 02.")
guardar_delta(df_concentracion, TABLA_CONCENTRACION)

# COMMAND ----------

# Leer el resultado persistido para no repetir el cálculo de concentración.
df_objetivos = spark.table(TABLA_CONCENTRACION).filter(
    (F.col("participacion_principal_pct") >= UMBRAL_CONCENTRACION_PCT)
    & (F.col("total_importado_usd") >= MINIMO_IMPORTACIONES_USD)
)

df_exportaciones = (
    df_global
    .join(F.broadcast(df_objetivos.select(*CLAVES)), on=CLAVES, how="left_semi")
    .groupBy(*CLAVES, "codigo_exportador", "exportador")
    .agg(
        F.sum("valor_usd").alias("exportaciones_mundiales_usd"),
        F.countDistinct("codigo_importador").alias("destinos"),
        F.sum(F.when(F.col("codigo_importador") == CODIGO_CR, F.col("valor_usd")).otherwise(0))
        .alias("exportaciones_a_cr_usd")
    )
)
orden_alternativas = Window.partitionBy(*CLAVES).orderBy(
    F.desc("exportaciones_mundiales_usd"), F.desc("destinos"), F.asc("codigo_exportador")
)
df_alternativas = (
    df_exportaciones
    .join(F.broadcast(df_objetivos), on=CLAVES, how="inner")
    .filter(
        (F.col("codigo_exportador") != CODIGO_CR)
        & (F.col("codigo_exportador") != F.col("codigo_exportador_principal"))
        & (F.col("exportaciones_mundiales_usd") > 0)
    )
    .withColumn("posicion_alternativa", F.row_number().over(orden_alternativas))
    .filter(F.col("posicion_alternativa") <= TOP_ALTERNATIVAS)
    .withColumnRenamed("codigo_exportador", "codigo_exportador_alternativo")
    .withColumnRenamed("exportador", "exportador_alternativo")
)
guardar_delta(df_alternativas, TABLA_ALTERNATIVAS)

df_guardado = spark.table(TABLA_ALTERNATIVAS)
resumen = df_guardado.agg(
    F.count("*").alias("filas"),
    F.countDistinct(*CLAVES).alias("casos_con_alternativas")
).first()
total_objetivos = df_objetivos.count()
print(f"Productos-año seleccionados: {total_objetivos}")
print(f"Productos-año con candidatos: {resumen['casos_con_alternativas']}")
print(f"Productos-año sin candidatos: {total_objetivos - resumen['casos_con_alternativas']}")
print(f"Filas guardadas: {resumen['filas']}")
print("Siguiente etapa: 04_validaciones")

