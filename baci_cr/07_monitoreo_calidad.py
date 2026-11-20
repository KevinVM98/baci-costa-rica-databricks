# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Historial de calidad de desarrollo
# MAGIC Revisa versiones concretas de las dos tablas analíticas existentes y agrega
# MAGIC los resultados a `workspace.baci_dev.monitoreo_calidad`. Usa las mismas
# MAGIC reglas que 04. Guarda los diagnósticos antes de fallar por una regla inválida.
# MAGIC
# MAGIC Este Job es independiente del pipeline y de la auditoría dev/prod. Cada
# MAGIC ejecución tiene un identificador propio; las reparaciones y reintentos
# MAGIC conservan el contexto del Job y generan una revisión nueva.

# COMMAND ----------

dbutils.widgets.dropdown("entorno", "dev", ["dev"], "Entorno de datos (solo desarrollo)")
BACI_ENTORNO = dbutils.widgets.get("entorno")

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

from monitoreo import ESQUEMA_HISTORIAL, ejecutar_monitoreo

for parametro in ("job_id", "job_run_id", "task_run_id", "intento"):
    dbutils.widgets.text(parametro, "")

contexto = {
    parametro: dbutils.widgets.get(parametro)
    for parametro in ("job_id", "job_run_id", "task_run_id", "intento")
}
resultados = ejecutar_monitoreo(
    spark, PARAMETROS_CALIDAD, entorno=ENTORNO, **contexto
)
display(spark.createDataFrame(resultados, schema=ESQUEMA_HISTORIAL).select(
    "id_revision", "tipo", "nombre", "valor", "estado"
))
