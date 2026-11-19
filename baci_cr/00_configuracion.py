# Databricks notebook source
# MAGIC %md
# MAGIC # BACI · Concentración de importaciones de Costa Rica
# MAGIC
# MAGIC Proyecto batch con PySpark, Databricks y tablas Delta. Datos BACI HS92, edición 202501, años 2016–2023.
# MAGIC
# MAGIC **Orden de reconstrucción:** `01_archivos_raw` → `02_comercio_enriquecido` → `03_indicadores_cr` → `04_validaciones`. `05_consultas_cr` permite explorar los resultados.
# MAGIC
# MAGIC Cada etapa ejecuta este notebook de configuración y lee archivos o tablas persistentes. No hay que conservar variables del notebook exploratorio. Mantené todos estos notebooks en la misma carpeta para que funcione `%run ./00_configuracion`.
# MAGIC
# MAGIC **Punto de partida:** el catálogo `workspace`, el esquema `baci`, el volumen `raw` y el ZIP ya existen. No es necesario volver a subir el ZIP. Esta versión parte del archivo descargado; no descarga ni programa tareas automáticamente.
# MAGIC
# MAGIC **Cómo ejecutar:** para reconstruir las tablas, ejecutá 01, 02, 03 y 04 en orden con Serverless. Si ya existen, podés consultar 05 o validar con 04. El Job encadena las cuatro etapas. El README del paquete explica cómo desplegar una copia mediante Databricks CLI.
# MAGIC
# MAGIC **Escritura:** 02 reemplaza `workspace.baci.comercio_enriquecido`; 03 reemplaza las tablas de concentración y alternativas. Es una reconstrucción completa, no una carga incremental. Cambiar años y reconstruir sustituye el período guardado por el nuevo período seleccionado. Cada tabla se confirma por separado: no hay una transacción conjunta para todo el flujo. Los resultados se consumen después de que 04 termine correctamente.
# MAGIC
# MAGIC **Compute:** importar archivos no procesa los datos. Reconstruir 02 vuelve a leer y escribir los datos globales y consume la cuota de cómputo de Free Edition. Para consultas cotidianas, ejecutá 05 sobre las tablas existentes.
# MAGIC
# MAGIC **Lectura de resultados:** `valor_usd` se obtiene multiplicando `v` por 1000. Las cantidades desconocidas permanecen nulas. Los códigos numéricos se conservan como texto de seis dígitos. El catálogo también conserva la excepción legada no WCO `9999AA`. Un país exportador puede reexportar mercancías: las exportaciones históricas no demuestran producción ni capacidad de suministro disponible. Un cero hacia CR significa ausencia de un flujo registrado en ese recorte de BACI.
# MAGIC
# MAGIC **Criterios exploratorios:** concentración ≥90 %, importaciones anuales de CR ≥US$1 millón y hasta tres alternativas por producto y año. Son parámetros del proyecto, no umbrales oficiales de riesgo. Las alternativas se ordenan por valor exportado mundial, con destinos y código del país como desempate.
# MAGIC
# MAGIC **Conteos observados en las ejecuciones compartidas por el usuario:** comercio global 86.758.220 filas; indicadores 33.727 filas, 4.524 productos distintos y ocho años; 957 productos-año seleccionados, 2.871 alternativas y cuatro reglas de calidad sin errores. Si cambiás parámetros o insumos, estos conteos pueden cambiar.
# MAGIC
# MAGIC El usuario reconstruyó y validó las tablas con los notebooks organizados y completó el Job manual con estado Succeeded el 18 de noviembre de 2026, en 8 min 29 s. La configuración portátil de bundle añadida al paquete requiere su propia validación y ejecución con Databricks CLI. Ver README.md y docs/VALIDACION.md.
# MAGIC
# MAGIC Fuentes: [CEPII BACI](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37), [importación de notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import), [uso de %run](https://docs.databricks.com/aws/en/notebooks/notebook-workflows), [CREATE OR REPLACE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using).

# COMMAND ----------

from pathlib import Path
import re

# Editá los parámetros aquí; todas las etapas leen esta configuración.
CATALOGO = "workspace"
ESQUEMA = "baci"
VOLUMEN = "raw"
CLASIFICACION = "HS92"
VERSION_BACI = "202501"
ANIO_INICIAL = 2016
ANIO_FINAL = 2023

CODIGO_CR = 188
UMBRAL_CONCENTRACION_PCT = 90
MINIMO_IMPORTACIONES_USD = 1_000_000
TOP_ALTERNATIVAS = 3

# False reutiliza archivos extraídos cuyo tamaño coincide con el ZIP.
# True fuerza su extracción de nuevo desde el ZIP.
REEXTRAER = False

# Validaciones de configuración, antes de leer o escribir datos.
for nombre in [CATALOGO, ESQUEMA, VOLUMEN]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", nombre):
        raise ValueError(f"Nombre de catálogo, esquema o volumen inválido: {nombre}")
if CLASIFICACION != "HS92" or VERSION_BACI != "202501":
    raise ValueError("Esta versión está preparada para BACI HS92 V202501.")
if not (1995 <= ANIO_INICIAL <= ANIO_FINAL <= 2023):
    raise ValueError("Elegí años entre 1995 y 2023, disponibles en este ZIP.")
if not (0 <= UMBRAL_CONCENTRACION_PCT <= 100):
    raise ValueError("La concentración debe estar entre 0 y 100.")
if MINIMO_IMPORTACIONES_USD < 0:
    raise ValueError("El importe mínimo debe ser no negativo.")
if not isinstance(TOP_ALTERNATIVAS, int) or TOP_ALTERNATIVAS < 1:
    raise ValueError("TOP_ALTERNATIVAS debe ser un entero positivo.")

ANIOS = list(range(ANIO_INICIAL, ANIO_FINAL + 1))
BASE_RAW = Path(f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}")
RUTA_ZIP = BASE_RAW / f"BACI_{CLASIFICACION}_V{VERSION_BACI}.zip"
CARPETA_TRADE = BASE_RAW / "trade"
CARPETA_REFERENCE = BASE_RAW / "reference"
RUTAS_TRADE = [
    str(CARPETA_TRADE / f"BACI_{CLASIFICACION}_Y{anio}_V{VERSION_BACI}.csv")
    for anio in ANIOS
]
RUTA_PAISES = str(CARPETA_REFERENCE / f"country_codes_V{VERSION_BACI}.csv")
RUTA_PRODUCTOS = str(
    CARPETA_REFERENCE / f"product_codes_{CLASIFICACION}_V{VERSION_BACI}.csv"
)

TABLA_COMERCIO = f"{CATALOGO}.{ESQUEMA}.comercio_enriquecido"
TABLA_CONCENTRACION = f"{CATALOGO}.{ESQUEMA}.concentracion_importaciones_cr"
TABLA_ALTERNATIVAS = f"{CATALOGO}.{ESQUEMA}.alternativas_importaciones_cr"
CLAVES = ["anio", "codigo_producto", "clasificacion", "version_baci"]


def guardar_delta(dataframe, tabla):
    """Reemplazar una tabla derivada sin acumular filas al repetir el proceso."""
    permitidas = {TABLA_COMERCIO, TABLA_CONCENTRACION, TABLA_ALTERNATIVAS}
    if tabla not in permitidas:
        raise ValueError(f"Tabla fuera de las salidas del proyecto: {tabla}")
    vista = "__baci_salida_" + tabla.split(".")[-1]
    dataframe.createOrReplaceTempView(vista)
    try:
        spark.sql(
            f"CREATE OR REPLACE TABLE {tabla} USING DELTA AS SELECT * FROM {vista}"
        ).collect()
    finally:
        spark.catalog.dropTempView(vista)
    print(f"Tabla disponible: {tabla}")


def comprobar_ceros(resultados):
    """Mostrar las reglas y detener el notebook cuando alguna falle."""
    for regla, cantidad in resultados.items():
        print(f"{regla}: {cantidad}")
    if any(cantidad != 0 for cantidad in resultados.values()):
        raise ValueError("Validación fallida: revisar las reglas con errores.")


print(f"Configuración: {CLASIFICACION} V{VERSION_BACI}, {ANIO_INICIAL}–{ANIO_FINAL}")

