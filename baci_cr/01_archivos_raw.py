# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Preparar los archivos originales
# MAGIC Lee el ZIP ya subido al volumen y extrae únicamente los años configurados y los catálogos. La extracción usa Python en el entorno remoto de Databricks y copia por bloques; no carga el ZIP completo en memoria.
# MAGIC
# MAGIC Si un archivo ya existe y su tamaño coincide con el registrado en el ZIP, se reutiliza. Esta comprobación de reutilización es por tamaño, no una verificación criptográfica. La opción `REEXTRAER` de 00 permite extraerlo de nuevo.
# MAGIC
# MAGIC Este bundle únicamente verifica y reutiliza archivos compartidos. No puede extraerlos ni reemplazarlos en dev o prod. Ante insumos faltantes, hace falta una preparación administrativa separada, fuera de este Job.

# COMMAND ----------

# Leer el parámetro en la tarea antes de cargar la configuración compartida.
dbutils.widgets.dropdown("entorno", "dev", ["dev"], "Entorno de datos (solo desarrollo)")
BACI_ENTORNO = dbutils.widgets.get("entorno")

# COMMAND ----------
# MAGIC %run ./00_configuracion

# COMMAND ----------

import shutil
import zipfile

if ENTORNO != "dev":
    raise ValueError("Extracción bloqueada: este bundle no permite preparar archivos en prod.")
if REEXTRAER:
    raise ValueError("REEXTRAER no se permite en dev: los insumos son de solo lectura.")


def extraer_si_necesario(zip_abierto, nombre, destino):
    """Extraer una entrada elegida explícitamente, sin usar rutas del ZIP."""
    entrada = zip_abierto.getinfo(nombre)
    destino = Path(destino)
    if not REEXTRAER and destino.is_file() and destino.stat().st_size == entrada.file_size:
        print(f"Ya disponible: {destino.name}")
        return "reutilizado"
    if ENTORNO == "dev":
        raise ValueError(
            f"Insumo ausente o con tamaño distinto al ZIP: {destino}. "
            "dev no modifica los archivos compartidos. Prepará los insumos "
            "mediante un proceso administrativo independiente antes de ejecutar dev."
        )
    destino.parent.mkdir(parents=True, exist_ok=True)
    try:
        with zip_abierto.open(entrada) as origen, destino.open("wb") as salida:
            shutil.copyfileobj(origen, salida, length=1024 * 1024)
        if destino.stat().st_size != entrada.file_size:
            raise IOError(f"Tamaño incompleto: {destino.name}")
    except Exception:
        # Quitar únicamente la copia de salida incompleta; el ZIP se conserva.
        destino.unlink(missing_ok=True)
        raise
    print(f"Extraído: {destino.name}")
    return "extraido"


archivos = [
    (f"BACI_{CLASIFICACION}_Y{anio}_V{VERSION_BACI}.csv", Path(ruta))
    for anio, ruta in zip(ANIOS, RUTAS_TRADE)
] + [
    (f"country_codes_V{VERSION_BACI}.csv", Path(RUTA_PAISES)),
    (f"product_codes_{CLASIFICACION}_V{VERSION_BACI}.csv", Path(RUTA_PRODUCTOS)),
    ("Readme.txt", CARPETA_REFERENCE / "Readme.txt"),
]

if not RUTA_ZIP.is_file():
    raise FileNotFoundError(f"Falta el ZIP en el volumen: {RUTA_ZIP}")

conteos = {"extraido": 0, "reutilizado": 0}
with zipfile.ZipFile(RUTA_ZIP) as archivo_zip:
    # Comprobar primero que están todas las entradas solicitadas.
    for nombre, _ in archivos:
        archivo_zip.getinfo(nombre)
    for nombre, destino in archivos:
        estado = extraer_si_necesario(archivo_zip, nombre, destino)
        conteos[estado] += 1

print(f"Archivos extraídos: {conteos['extraido']}")
print(f"Archivos reutilizados: {conteos['reutilizado']}")
print(f"Siguiente etapa: 02_comercio_enriquecido, con entorno={ENTORNO}")

