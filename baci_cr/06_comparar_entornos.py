# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Auditoría de desarrollo frente a producción
# MAGIC Compara **solo las dos tablas analíticas** de `workspace.baci_dev` y `workspace.baci`.
# MAGIC No vuelve a procesar la tabla global de 86,7 millones de filas, no ejecuta
# MAGIC ninguna escritura y no altera el dashboard.
# MAGIC
# MAGIC La comparación usa `EXCEPT ALL` en ambos sentidos: detecta valores
# MAGIC diferentes incluso cuando el número de filas coincide y conserva duplicados.
# MAGIC Los datos deben ser idénticos si ambos entornos se reconstruyeron con
# MAGIC los mismos archivos, parámetros y lógica. Un cambio intencional puede
# MAGIC producir diferencias legítimas: revisá los ejemplos antes de publicar.
# MAGIC
# MAGIC Ejecutar este notebook directamente o usar el Job independiente
# MAGIC `baci_cr_auditoria`. No es parte de la reconstrucción de cuatro tareas.

# COMMAND ----------

import json

from comparacion import comparar_contenido

TABLAS_ANALITICAS = (
    "concentracion_importaciones_cr",
    "alternativas_importaciones_cr",
)
MAX_EJEMPLOS = 3
# La auditoría sirve como puerta de control: termina con error ante diferencias.
FALLAR_SI_DIFERENCIAS = True

if not isinstance(FALLAR_SI_DIFERENCIAS, bool):
    raise ValueError("FALLAR_SI_DIFERENCIAS debe ser True o False.")

# COMMAND ----------

resultados = {}
for nombre in TABLAS_ANALITICAS:
    ruta_dev = f"workspace.baci_dev.{nombre}"
    ruta_prod = f"workspace.baci.{nombre}"
    print(f"Comparando {ruta_dev} contra {ruta_prod}")
    resultado = comparar_contenido(
        spark.table(ruta_dev), spark.table(ruta_prod), max_ejemplos=MAX_EJEMPLOS
    )
    resultados[nombre] = resultado
    print(f"  Filas exclusivas de dev:  {resultado['filas_solo_dev']}")
    print(f"  Filas exclusivas de prod: {resultado['filas_solo_prod']}")
    print(f"  Resultado: {'IGUAL' if resultado['iguales'] else 'DIFERENTE'}")
    if not resultado["iguales"]:
        print(json.dumps({
            "ejemplos_solo_dev": resultado["ejemplos_solo_dev"],
            "ejemplos_solo_prod": resultado["ejemplos_solo_prod"],
        }, ensure_ascii=False, indent=2, default=str))

diferentes = [tabla for tabla, resultado in resultados.items() if not resultado["iguales"]]
if diferentes and FALLAR_SI_DIFERENCIAS:
    raise ValueError(
        f"Auditoría con diferencias en: {', '.join(diferentes)}. "
        "Revisá los ejemplos; no se modificó ninguna tabla."
    )
print("Auditoría completada. Todas las tablas coinciden." if not diferentes
      else "Auditoría completada con diferencias documentadas. No se modificaron tablas.")
