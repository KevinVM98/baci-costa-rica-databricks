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
from transformaciones import (
    calcular_alternativas,
    calcular_concentracion,
    seleccionar_productos,
)

df_global = spark.table(TABLA_COMERCIO).filter(
    F.col("anio").between(ANIO_INICIAL, ANIO_FINAL)
    & (F.col("clasificacion") == CLASIFICACION)
    & (F.col("version_baci") == VERSION_BACI)
)
df_concentracion = calcular_concentracion(
    df_global, claves=CLAVES, codigo_cr=CODIGO_CR
)

if not df_concentracion.limit(1).count():
    raise ValueError("No hay indicadores para la configuración: revisá la etapa 02.")
guardar_delta(df_concentracion, TABLA_CONCENTRACION)

# COMMAND ----------

# Leer el resultado persistido para no repetir el cálculo de concentración.
df_objetivos = seleccionar_productos(
    spark.table(TABLA_CONCENTRACION),
    umbral_pct=UMBRAL_CONCENTRACION_PCT,
    minimo_importaciones_usd=MINIMO_IMPORTACIONES_USD
)

df_alternativas = calcular_alternativas(
    df_global, df_objetivos, claves=CLAVES,
    codigo_cr=CODIGO_CR, top_alternativas=TOP_ALTERNATIVAS
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
