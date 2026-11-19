# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validaciones de las tablas analíticas
# MAGIC Aplica las cuatro reglas ya probadas en el notebook exploratorio. Revisa unicidad, estructura de los rankings, países excluidos, coherencia de montos y pertenencia a los casos seleccionados. Una falla genera `ValueError` para que la tarea de validación del Job termine con error.
# MAGIC
# MAGIC Lee únicamente las tablas analíticas guardadas. Se puede ejecutar ahora sobre las tablas existentes. No requiere ejecutar manualmente 00: su configuración se carga en la siguiente celda.
# MAGIC
# MAGIC Un caso puede tener menos candidatos que el máximo configurado; las posiciones deben ser consecutivas desde 1. La cobertura de candidatos se informa al final. Estas reglas no comprueban, por sí solas, exactitud económica ni disponibilidad de suministro.

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

from pyspark.sql import functions as F

df_indicadores = spark.table(TABLA_CONCENTRACION)
df_alternativas = spark.table(TABLA_ALTERNATIVAS)

# Evitar que una tabla de indicadores vacía apruebe todas las reglas por ausencia de filas.
if not df_indicadores.limit(1).count():
    raise ValueError("La tabla de indicadores está vacía: revisá 03.")

# Comprobar que las tablas corresponden al período y edición de 00.
cobertura = df_indicadores.select("anio", "clasificacion", "version_baci").distinct().collect()
if {r["anio"] for r in cobertura} != set(ANIOS) or any(
    r["clasificacion"] != CLASIFICACION or r["version_baci"] != VERSION_BACI
    for r in cobertura
):
    raise ValueError("La cobertura guardada no coincide con 00: reconstruí 02 y 03 con la misma configuración.")

errores = {}
errores["indicadores_duplicados"] = (
    df_indicadores.groupBy(*CLAVES).count().filter("count > 1").count()
)
df_revision_ranking = df_alternativas.groupBy(*CLAVES).agg(
    F.count("*").alias("filas"),
    F.countDistinct("codigo_exportador_alternativo").alias("paises"),
    F.countDistinct("posicion_alternativa").alias("posiciones"),
    F.min("posicion_alternativa").alias("posicion_minima"),
    F.max("posicion_alternativa").alias("posicion_maxima")
)
errores["rankings_invalidos"] = df_revision_ranking.filter(f"""
    filas > {TOP_ALTERNATIVAS}
    OR paises != filas
    OR posiciones != filas
    OR posicion_minima != 1
    OR posicion_maxima != filas
""").count()

errores["filas_con_paises_o_valores_invalidos"] = df_alternativas.filter(f"""
    codigo_exportador_alternativo = {CODIGO_CR}
    OR codigo_exportador_alternativo = codigo_exportador_principal
    OR exportaciones_mundiales_usd IS NULL
    OR exportaciones_mundiales_usd <= 0
    OR exportaciones_a_cr_usd IS NULL
    OR exportaciones_a_cr_usd < 0
    OR exportaciones_a_cr_usd > exportaciones_mundiales_usd
    OR destinos IS NULL OR destinos < 1
""").count()

df_objetivos_validos = df_indicadores.filter(
    (F.col("participacion_principal_pct") >= UMBRAL_CONCENTRACION_PCT)
    & (F.col("total_importado_usd") >= MINIMO_IMPORTACIONES_USD)
)
claves_con_principal = CLAVES + ["codigo_exportador_principal"]
errores["alternativas_sin_caso_valido"] = df_alternativas.join(
    df_objetivos_validos.select(*claves_con_principal),
    on=claves_con_principal, how="left_anti"
).count()

comprobar_ceros(errores)
print("Validación completada: las cuatro reglas se cumplen.")

# COMMAND ----------

display(df_indicadores.agg(
    F.count("*").alias("filas_indicadores"),
    F.countDistinct("codigo_producto").alias("productos_en_el_periodo"),
    F.countDistinct("anio").alias("anios"),
    F.min("anio").alias("anio_minimo"), F.max("anio").alias("anio_maximo")
))
casos_sin_candidato = (
    df_objetivos_validos.select(*CLAVES)
    .join(df_alternativas.select(*CLAVES).distinct(), on=CLAVES, how="left_anti")
    .count()
)
print(f"Productos-año seleccionados sin candidatos: {casos_sin_candidato}")
print("La validación terminó. Podés explorar los resultados con 05_consultas_cr.")

