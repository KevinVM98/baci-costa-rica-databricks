"""Historial de calidad: lecturas por versión y una escritura append en dev."""

from datetime import datetime, timezone
import json
import re
from uuid import uuid4

from calidad import VERSION_REGLAS, evaluar_calidad


TABLA_HISTORIAL = "workspace.baci_dev.monitoreo_calidad"
TABLA_INDICADORES = "workspace.baci_dev.concentracion_importaciones_cr"
TABLA_ALTERNATIVAS = "workspace.baci_dev.alternativas_importaciones_cr"
ESQUEMA_HISTORIAL = """
    id_revision STRING, fecha_utc TIMESTAMP, entorno STRING,
    job_id STRING, job_run_id STRING, task_run_id STRING, intento INT,
    tabla_indicadores STRING, version_indicadores BIGINT,
    tabla_alternativas STRING, version_alternativas BIGINT,
    version_reglas STRING, parametros_json STRING, estado_revision STRING,
    tipo STRING, nombre STRING, valor BIGINT, estado STRING, detalle STRING
"""


def comprobar_destinos(entorno):
    """Rechazar desvíos antes de usar Spark, incluso si se mutan constantes."""
    if entorno != "dev" or (
        TABLA_HISTORIAL != "workspace.baci_dev.monitoreo_calidad"
        or TABLA_INDICADORES != "workspace.baci_dev.concentracion_importaciones_cr"
        or TABLA_ALTERNATIVAS != "workspace.baci_dev.alternativas_importaciones_cr"
    ):
        raise ValueError("Monitoreo bloqueado: solo se permiten sus tablas fijas de dev.")


def normalizar_contexto(job_id="", job_run_id="", task_run_id="", intento=""):
    """Widgets vacíos permiten uso manual; rechazar referencias sin resolver."""
    resultado = {}
    for nombre, valor in (
        ("job_id", job_id), ("job_run_id", job_run_id),
        ("task_run_id", task_run_id), ("intento", intento),
    ):
        texto = str(valor).strip() if valor is not None else ""
        if texto and not re.fullmatch(r"[0-9]+", texto):
            raise ValueError(f"Metadato {nombre} inválido: se esperaba un identificador numérico.")
        resultado[nombre] = (int(texto) if nombre == "intento" else texto) if texto else None
    return resultado


def obtener_version(spark, tabla):
    if tabla not in (
        "workspace.baci_dev.concentracion_importaciones_cr",
        "workspace.baci_dev.alternativas_importaciones_cr",
    ):
        raise ValueError("Tabla de lectura no permitida para monitoreo.")
    fila = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first()
    if fila is None or type(fila["version"]) is not int or fila["version"] < 0:
        raise ValueError("No se pudo obtener una versión Delta válida.")
    return fila["version"]


def guardar_historial(spark, filas, entorno):
    """Añadir un lote completo de resultados. Nunca reemplazar el historial."""
    comprobar_destinos(entorno)
    if not filas:
        raise ValueError("No se puede guardar una revisión sin resultados.")
    if any(fila["entorno"] != "dev" for fila in filas):
        raise ValueError("Los registros de monitoreo deben pertenecer a dev.")
    dataframe = spark.createDataFrame(filas, schema=ESQUEMA_HISTORIAL)
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.baci_dev").collect()
    dataframe.write.format("delta").mode("append").saveAsTable(
        "workspace.baci_dev.monitoreo_calidad"
    )


def ejecutar_monitoreo(spark, parametros, *, entorno="dev", **contexto):
    """Guardar diagnóstico antes de propagar fallas de calidad o de lectura.

    Un UUID identifica cada invocación; reintentos generan revisiones nuevas.
    Si el proceso se cancela o falla el almacenamiento, no se garantiza un
    registro. El historial de Jobs sigue siendo la fuente para esas fallas.
    """
    comprobar_destinos(entorno)
    contexto = normalizar_contexto(**contexto)
    cabecera = {
        "id_revision": str(uuid4()),
        "fecha_utc": datetime.now(timezone.utc),
        "entorno": entorno,
        **contexto,
        "tabla_indicadores": TABLA_INDICADORES,
        "version_indicadores": None,
        "tabla_alternativas": TABLA_ALTERNATIVAS,
        "version_alternativas": None,
        "version_reglas": VERSION_REGLAS,
        "parametros_json": json.dumps(parametros, sort_keys=True, ensure_ascii=False),
    }
    fase = "leer_versiones"
    try:
        cabecera["version_indicadores"] = obtener_version(spark, TABLA_INDICADORES)
        cabecera["version_alternativas"] = obtener_version(spark, TABLA_ALTERNATIVAS)
        fase = "leer_tablas"
        indicadores = spark.read.option(
            "versionAsOf", cabecera["version_indicadores"]
        ).table(TABLA_INDICADORES)
        alternativas = spark.read.option(
            "versionAsOf", cabecera["version_alternativas"]
        ).table(TABLA_ALTERNATIVAS)
        fase = "evaluar_reglas"
        evaluacion = evaluar_calidad(indicadores, alternativas, **parametros)
    except Exception as error:
        # No guardar mensajes que puedan incluir muestras de datos o secretos.
        # El traceback original permanece en la salida de la tarea.
        fila = {
            **cabecera, "estado_revision": "ERROR", "tipo": "ERROR_TECNICO",
            "nombre": fase, "valor": None, "estado": "ERROR",
            "detalle": type(error).__name__,
        }
        guardar_historial(spark, [fila], entorno)
        print(f"Revisión {cabecera['id_revision']}: ERROR ({fase}).")
        raise

    fallo = any(cantidad != 0 for cantidad in evaluacion["errores"].values())
    cabecera["estado_revision"] = "FALLIDO" if fallo else "APROBADO"
    filas = [
        {**cabecera, "tipo": "REGLA", "nombre": nombre, "valor": cantidad,
         "estado": "FALLIDO" if cantidad else "APROBADO", "detalle": None}
        for nombre, cantidad in evaluacion["errores"].items()
    ] + [
        {**cabecera, "tipo": "METRICA", "nombre": nombre, "valor": cantidad,
         "estado": "INFORMATIVO", "detalle": None}
        for nombre, cantidad in evaluacion["metricas"].items()
    ]
    guardar_historial(spark, filas, entorno)
    print(f"Revisión {cabecera['id_revision']}: {cabecera['estado_revision']}.")
    if fallo:
        fallidas = [nombre for nombre, cantidad in evaluacion["errores"].items() if cantidad]
        raise ValueError(
            f"Calidad fallida; diagnóstico guardado con id_revision={cabecera['id_revision']}. "
            f"Reglas: {', '.join(fallidas)}"
        )
    return filas
