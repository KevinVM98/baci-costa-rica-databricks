# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Comercio global enriquecido
# MAGIC Lee los CSV anuales con un esquema explícito, valida los campos requeridos y agrega nombres de países y productos mediante tres uniones izquierdas con catálogos pequeños.
# MAGIC
# MAGIC Mantiene todos los flujos globales del período. La unidad es año–exportador–importador–producto. Convierte miles de USD a USD una sola vez. Conserva nulos en toneladas. Al ejecutar la etapa completa, **reemplaza** `comercio_enriquecido` con el período configurado.
# MAGIC
# MAGIC El catálogo se lee como UTF-8. Algunos nombres de países contienen texto UTF-8 que fue interpretado como Latin-1 antes de guardarse en el CSV. Se revierte una capa de esa conversión al preparar el catálogo, con decodificación estricta y sin modificar los archivos originales. Los nombres corregidos se usan tanto para exportadores como para importadores.

# COMMAND ----------

# Leer el parámetro en la tarea antes de cargar la configuración compartida.
dbutils.widgets.dropdown("entorno", "dev", ["dev"], "Entorno de datos (solo desarrollo)")
BACI_ENTORNO = dbutils.widgets.get("entorno")

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

from catalogos import normalizar_nombre_pais
from transformaciones import enriquecer_comercio

faltantes = [ruta for ruta in RUTAS_TRADE + [RUTA_PAISES, RUTA_PRODUCTOS] if not Path(ruta).is_file()]
if faltantes:
    raise FileNotFoundError(f"Ejecutá 01 primero. Faltan archivos: {faltantes}")

df_comercio = (
    spark.read
    .option("header", True)
    .option("nullValue", "")
    .option("enforceSchema", False)
    .option("mode", "FAILFAST")
    .schema("t INT, i INT, j INT, k STRING, v DECIMAL(20,3), q DOUBLE")
    .csv(RUTAS_TRADE)
)

requeridos = ["t", "i", "j", "k", "v"]
incompleta = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in requeridos])
df_resumen = df_comercio.groupBy("t").agg(
    F.count("*").alias("total_filas"),
    F.sum(F.when(incompleta, 1).otherwise(0)).alias("filas_incompletas"),
    F.sum(F.when(~F.trim(F.col("k")).rlike(r"^[0-9]{1,6}$"), 1).otherwise(0)).alias("codigos_invalidos"),
    F.sum(F.when(F.col("q").isNull(), 1).otherwise(0)).alias("cantidades_faltantes")
)
# Se trae únicamente el resumen de un registro por año, no los datos globales.
resumen = df_resumen.collect()
total_origen = sum(fila["total_filas"] for fila in resumen)
anios_observados = {fila["t"] for fila in resumen}
for fila in sorted(resumen, key=lambda r: -1 if r["t"] is None else r["t"]):
    print(fila.asDict())
if total_origen == 0 or anios_observados != set(ANIOS):
    raise ValueError(f"Cobertura anual inesperada: {anios_observados}")
if any(fila["filas_incompletas"] or fila["codigos_invalidos"] for fila in resumen):
    raise ValueError("Hay campos requeridos nulos o códigos HS inválidos.")

# El CSV puede omitir ceros iniciales; normalizar antes de unir por código HS.
df_comercio = df_comercio.withColumn("k", F.lpad(F.trim(F.col("k")), 6, "0"))

# COMMAND ----------

opciones_catalogos = {
    "header": "true", "escape": '"', "multiLine": "true",
    "nullValue": "", "enforceSchema": "false", "mode": "FAILFAST",
    "encoding": "UTF-8"
}
df_paises = (
    spark.read.options(**opciones_catalogos)
    .schema("country_code INT, country_name STRING, country_iso2 STRING, country_iso3 STRING")
    .csv(RUTA_PAISES)
)

# Solo se recogen los nombres distintos del catálogo pequeño, no el comercio global.
# El reemplazo posterior se ejecuta en Spark, sin una UDF de Python.
correcciones_paises = {}
for fila in df_paises.select("country_name").distinct().collect():
    original = fila["country_name"]
    corregido = normalizar_nombre_pais(original)
    if corregido != original:
        correcciones_paises[original] = corregido

if correcciones_paises:
    df_paises = df_paises.replace(correcciones_paises, subset=["country_name"])

print(f"Nombres de países corregidos: {len(correcciones_paises)}")
for original, corregido in sorted(correcciones_paises.items()):
    print(f"  {ascii(original)} -> {corregido}")

# Verificar el catálogo resultante antes de usarlo en las dos uniones de países.
nombres_sin_normalizar = sum(
    normalizar_nombre_pais(fila["country_name"]) != fila["country_name"]
    for fila in df_paises.select("country_name").distinct().collect()
)

df_productos = (
    spark.read.options(**opciones_catalogos)
    .schema("code STRING, description STRING")
    .csv(RUTA_PRODUCTOS)
)

# El catálogo incluye 9999AA: "Commodities not specified according to kind",
# identificado por la fuente como LEGACY NON-WCO CODE. Se conserva como texto;
# no se trata como un código HS numérico ni se convierte a 999999.
codigo_limpio = F.trim(F.col("code"))
es_codigo_numerico = codigo_limpio.rlike(r"^[0-9]{1,6}$")
es_codigo_legado = codigo_limpio == F.lit("9999AA")

# Validar antes de normalizar para no truncar códigos de más de seis dígitos.
if df_productos.filter(
    F.col("code").isNull()
    | ~(es_codigo_numerico | es_codigo_legado)
    | F.col("description").isNull()
).limit(1).count():
    raise ValueError("Hay códigos o descripciones inválidos en el catálogo de productos.")

# Completar ceros solo en los códigos numéricos; conservar 9999AA exactamente.
df_productos = df_productos.withColumn(
    "code",
    F.when(es_codigo_numerico, F.lpad(codigo_limpio, 6, "0"))
    .otherwise(codigo_limpio)
)

comprobar_ceros({
    "paises_duplicados": df_paises.groupBy("country_code").count().filter("count > 1").count(),
    "productos_duplicados": df_productos.groupBy("code").count().filter("count > 1").count(),
    "paises_sin_clave_o_nombre": df_paises.filter("country_code IS NULL OR country_name IS NULL").count(),
    "paises_con_nombres_sin_normalizar": nombres_sin_normalizar,
})

# La función compartida también se ejecuta en las pruebas automáticas.
df_enriquecido = enriquecer_comercio(
    df_comercio, df_paises, df_productos,
    clasificacion=CLASIFICACION, version_baci=VERSION_BACI
)

# Validar el enriquecimiento antes de publicar la tabla.
revision = df_enriquecido.agg(
    F.count("*").alias("filas"),
    *[
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias("nulos_" + c)
        for c in ["exportador", "importador", "producto"]
    ]
).first().asDict()
if revision["filas"] != total_origen or any(revision["nulos_" + c] for c in ["exportador", "importador", "producto"]):
    raise ValueError(f"La unión alteró la cantidad de filas o dejó referencias sin resolver: {revision}")

# COMMAND ----------

guardar_delta(df_enriquecido, TABLA_COMERCIO)
total_guardado = spark.table(TABLA_COMERCIO).count()
if total_guardado != total_origen:
    raise ValueError(f"Conteo guardado inesperado: {total_guardado}, origen: {total_origen}")
print(f"Filas almacenadas: {total_guardado}")
print("Siguiente etapa: 03_indicadores_cr")
