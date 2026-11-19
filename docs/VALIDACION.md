# Registro de validación

Fecha de referencia: **18 de noviembre de 2026**, hora de Costa Rica (UTC−6).

## Evidencia de Databricks compartida por el usuario

| Comprobación | Resultado observado |
|---|---|
| Reutilización en etapa 01 | 0 archivos extraídos, 11 reutilizados. |
| Enriquecimiento en etapa 02 | 86.758.220 filas; cero nombres de exportador, importador o producto faltantes. |
| Catálogos en etapa 02 | Cero códigos duplicados; cero países sin clave o nombre. |
| Tabla global guardada | 86.758.220 filas almacenadas. |
| Alternativas en etapa 03 | 957 casos seleccionados, 957 con candidatos, 0 sin candidatos, 2.871 filas. |
| Cobertura en etapa 04 | 33.727 indicadores, 4.524 productos, ocho años: 2016–2023. |
| Controles de etapa 04 | Finalización correcta de las validaciones. |
| Grafo del Job | Cuatro tareas encadenadas: 01, 02, 03 y 04; Serverless. |
| Ejecución del Job creado en la interfaz | `Succeeded`; inicio mostrado: 18/11/2026 a las 18:43; duración: 8 min 29 s. |

La duración corresponde a una ejecución observada. No es una medición repetida ni una garantía de tiempo de ejecución. Las capturas de la interfaz no se incluyen en el paquete.

La ejecución de esa tabla corresponde al Job creado manualmente en el workspace. Sus rutas absolutas se recibieron en el YAML exportado por el usuario. La versión portátil usa rutas relativas a los notebooks de este proyecto y un destino `dev`; su ejecución también se comprobó mediante la CLI, como se registra abajo.

## Corrección aplicada al catálogo

La validación inicial rechazaba cualquier código no numérico. El diagnóstico del usuario mostró una única fila: `9999AA`, con descripción `Commodities not specified according to kind **LEGACY NON-WCO CODE**`.

Se admitió esa excepción exacta en el catálogo y se conservó su código. La normalización con ceros se aplica a los códigos numéricos. La reconstrucción posterior conservó el conteo y dejó cero referencias faltantes. También se eliminó de la celda del usuario una copia antigua del bloque de validación que había quedado pegada debajo del bloque corregido.

## Revisión local de este paquete

- Sintaxis Python de los seis notebooks comprobada, sin ejecutar Spark localmente.
- YAML de la configuración principal y del Job analizado correctamente.
- Cuatro claves de tareas únicas; cada dependencia apunta a la etapa anterior.
- Rutas relativas de las cuatro tareas verificadas contra los archivos incluidos.
- `00_configuracion.py` presente junto a los notebooks que lo cargan con `%run`.
- Cola y modo de rendimiento conservados del YAML recibido; ejecución simultánea limitada a una por Job y condición `ALL_SUCCESS` explícita en las tres tareas dependientes.
- Ausencia de correos, rutas personales fijas, credenciales y archivos de datos en el paquete de distribución.
- Integridad del ZIP verificada.

## Validación del despliegue portátil

El usuario ejecutó los comandos desde PowerShell en Windows y compartió las salidas de la CLI y una captura de la ejecución terminada el **18 de noviembre de 2026**.

| Comprobación | Resultado observado |
|---|---|
| `databricks --version` | `Databricks CLI v1.26.0`. |
| Autenticación interactiva con perfil `BACI` | `Profile BACI was successfully saved`. |
| `databricks bundle validate -t dev -p BACI` | Bundle `baci_cr`, destino `dev`, `Validation OK!`. |
| `databricks bundle deploy -t dev -p BACI` | 0 archivos subidos o eliminados; 0 recursos creados, modificados o eliminados; 1 recurso sin cambios. |
| `databricks bundle run -t dev -p BACI baci_cr_pipeline` | Job de desarrollo con estado final `TERMINATED SUCCESS`; salida con las tareas 01, 02, 03 y 04. |

El despliegue mostrado encontró los archivos y el recurso ya sincronizados. La captura de la terminal muestra `RUNNING` a las 20:28:09 y `TERMINATED SUCCESS` a las 20:31:06. Estas marcas corresponden a los mensajes de la CLI, no a una medición independiente de la duración de cada tarea.

La ejecución del bundle reutilizó el workspace del proyecto, con catálogo, esquema, volumen y ZIP de origen ya preparados. Esta evidencia confirma la validación, el despliegue y la ejecución desde los archivos del proyecto en ese entorno; una instalación en un workspace vacío sigue requiriendo la preparación descrita en el README. Los conteos de la primera tabla de este documento proceden de las salidas anteriores: la captura final de la CLI no los vuelve a mostrar.

Los comandos remotos se ejecutaron en la computadora y el workspace del usuario. La revisión de los archivos y su empaquetado se realizó por separado en el entorno local de preparación. Esta actualización cambia únicamente `README.md` y `docs/VALIDACION.md`.

Las reglas de datos del notebook 04 cubren la coherencia implementada; no garantizan por sí solas calidad económica completa ni disponibilidad comercial de los países candidatos.
