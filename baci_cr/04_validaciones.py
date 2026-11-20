# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validaciones de las tablas analíticas
# MAGIC Reutiliza las reglas de calidad compartidas con el Job de monitoreo. Revisa unicidad, estructura de los rankings, países excluidos, coherencia de montos y pertenencia a los casos seleccionados. Una falla genera `ValueError` para que la tarea de validación del Job termine con error.
# MAGIC
# MAGIC Lee únicamente las tablas analíticas guardadas. Se puede ejecutar ahora sobre las tablas existentes. No requiere ejecutar manualmente 00: su configuración se carga en la siguiente celda.
# MAGIC
# MAGIC Un caso puede tener menos candidatos que el máximo configurado; las posiciones deben ser consecutivas desde 1. La cobertura de candidatos se informa al final. Estas reglas no comprueban, por sí solas, exactitud económica ni disponibilidad de suministro.

# COMMAND ----------

# Leer el parámetro en la tarea antes de cargar la configuración compartida.
dbutils.widgets.dropdown("entorno", "dev", ["dev"], "Entorno de datos (solo desarrollo)")
BACI_ENTORNO = dbutils.widgets.get("entorno")

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

from calidad import evaluar_calidad

df_indicadores = spark.table(TABLA_CONCENTRACION)
df_alternativas = spark.table(TABLA_ALTERNATIVAS)
evaluacion = evaluar_calidad(
    df_indicadores, df_alternativas, **PARAMETROS_CALIDAD
)
comprobar_ceros(evaluacion["errores"])
print("Validación completada: los controles de calidad se cumplen.")

# COMMAND ----------

display(spark.createDataFrame([evaluacion["metricas"]]))
print(f"Productos-año seleccionados sin candidatos: {evaluacion['metricas']['casos_sin_candidato']}")
print("La validación terminó. Podés explorar los resultados con 05_consultas_cr.")
