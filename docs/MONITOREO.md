# Historial de calidad de desarrollo

El Job independiente `baci_cr_monitoreo` ejecuta `07_monitoreo_calidad.py`.
Lee las dos tablas analíticas de `workspace.baci_dev` y añade un lote de
resultados a la tabla Delta `workspace.baci_dev.monitoreo_calidad`.
No procesa CSV ni reconstruye comercio global. Los Jobs del pipeline y de
auditoría dev/prod conservan su ejecución independiente.

## Reglas compartidas

`04_validaciones` y `07_monitoreo_calidad` llaman a `calidad.evaluar_calidad`
con `PARAMETROS_CALIDAD`, definidos en `00_configuracion`. La versión de reglas
actual es `1`; debe actualizarse si cambia su significado.

| Regla | Qué representa un valor distinto de cero |
|---|---|
| `indicadores_vacios` | Indicador binario: la tabla de concentración no contiene filas. |
| `cobertura_incorrecta` | Indicador binario: faltan/sobran años o difiere la clasificación/edición configurada. |
| `indicadores_duplicados` | Cantidad de claves de producto-año-edición con más de una fila. |
| `rankings_invalidos` | Cantidad de grupos con países/posiciones repetidos, huecos o más candidatos de los permitidos. |
| `filas_con_paises_o_valores_invalidos` | Filas con países excluidos o con montos/destinos inválidos según las reglas de 04. |
| `alternativas_sin_caso_valido` | Filas que no corresponden a un producto seleccionado y su exportador principal. |

Las siete métricas informativas son: filas de indicadores, productos distintos,
número de años, año mínimo, año máximo, filas de alternativas y casos sin
candidato. La ausencia de candidatos sigue siendo informativa: no se inventa
una alternativa ni se considera por sí sola un error.

Una evaluación completada añade **13 filas**: seis reglas y siete métricas.
Estas filas describen una revisión, no trece ejecuciones distintas.

## Versiones y contexto

Antes de evaluar, el Job obtiene las versiones Delta actuales de ambas tablas
con `DESCRIBE HISTORY ... LIMIT 1` y las lee explícitamente mediante
`versionAsOf`. El historial registra esos números junto a los nombres de las
tablas. Así, una escritura posterior no cambia las lecturas de esa revisión.

Cada invocación genera un UUID `id_revision` y una fecha UTC. Los parámetros
del Job aportan `job_id`, `job_run_id`, `task_run_id` e `intento`; los valores
dinámicos se resuelven en Databricks. Si el notebook se ejecuta manualmente,
esos campos pueden quedar nulos. Una referencia sin resolver se rechaza antes
de consultar Spark. Estos identificadores son contexto operativo, no prueba
criptográfica de identidad.

`parametros_json` conserva período, edición, clasificación, claves, umbrales,
país de análisis y límite de alternativas. `version_reglas` permite distinguir
cambios en la definición de los controles.

Las versiones se capturan de forma independiente: no constituyen una
transacción conjunta de ambas tablas. Ejecutar el monitoreo cuando el pipeline
haya terminado. Tampoco se conserva una copia de los datos: volver a consultar
versiones antiguas depende de la retención de Delta y de que no se elimine o
recree la tabla. No borrar/recrear tablas durante la revisión.

## Estados y fallas

| Campo | Valores |
|---|---|
| `estado_revision` | `APROBADO`, `FALLIDO` o `ERROR`, repetido en las filas de la revisión. |
| `tipo` | `REGLA`, `METRICA` o `ERROR_TECNICO`. |
| `estado` | Resultado individual: `APROBADO`, `FALLIDO`, `INFORMATIVO` o `ERROR`. |

Si una regla falla, se escriben las seis reglas y siete métricas antes de
generar `ValueError`. El Job queda fallido y el diagnóstico sigue consultable.
Si ocurre una excepción al obtener versiones, leer tablas o evaluar reglas,
se intenta añadir una fila `ERROR_TECNICO`; las versiones no obtenidas quedan
nulas. Se guarda la fase y clase de excepción, no su mensaje ni muestras de
datos. El traceback original permanece en la salida del Job.

Si no se puede escribir el historial, la ejecución falla. Cancelaciones,
terminaciones abruptas, errores de configuración previos a la evaluación y
caídas de infraestructura pueden no producir ninguna fila. El historial de
Jobs complementa esta tabla; no se presenta como un registro completo de
todas las fallas posibles.

La escritura usa **append** y un lote por revisión. Cada reintento o reparación
genera una revisión nueva, aunque tenga el mismo `job_run_id`; se distinguen
por `id_revision`, `task_run_id` e `intento`. No se ofrece garantía de
exactamente una escritura ante resultados ambiguos de red. La política de
retención del historial queda a cargo del proyecto; este Job no borra filas.

## Ejecutar

Desde la raíz del repositorio y con el perfil local `BACI`:

```powershell
databricks bundle validate -t dev -p BACI
databricks bundle deploy -t dev -p BACI
databricks bundle run -t dev -p BACI baci_cr_monitoreo
```

El despliegue incorpora un tercer Job, sin iniciar el pipeline. La ejecución
del monitoreo requiere las dos tablas analíticas de desarrollo ya disponibles
y permiso para crear/escribir `workspace.baci_dev.monitoreo_calidad`. Su primera
escritura crea la tabla si no existe. Los datos de las tablas de origen se leen.

Este flujo solo admite `entorno=dev`. `guardar_delta` mantiene su lista de tres
salidas derivadas y no puede reemplazar el historial; este usa una rutina
separada, un destino literal y modo append. Las guardas también rechazan
constantes alteradas para apuntar a producción. Esto es protección del código,
no aislamiento de permisos de Unity Catalog.

## Consultar resultados

Una fila por revisión:

```sql
SELECT
  id_revision,
  MAX(fecha_utc) AS fecha_utc,
  MAX(job_run_id) AS job_run_id,
  MAX(intento) AS intento,
  MAX(version_indicadores) AS version_indicadores,
  MAX(version_alternativas) AS version_alternativas,
  MAX(estado_revision) AS estado_revision,
  SUM(CASE WHEN tipo = 'REGLA' AND estado = 'FALLIDO' THEN 1 ELSE 0 END) AS reglas_fallidas
FROM workspace.baci_dev.monitoreo_calidad
GROUP BY id_revision
ORDER BY fecha_utc DESC;
```

Diagnósticos que necesitan revisión:

```sql
SELECT fecha_utc, id_revision, job_run_id, nombre, valor, estado, detalle
FROM workspace.baci_dev.monitoreo_calidad
WHERE estado IN ('FALLIDO', 'ERROR')
ORDER BY fecha_utc DESC, nombre;
```

Detalle de la última revisión:

```sql
WITH ultima AS (
  SELECT id_revision
  FROM workspace.baci_dev.monitoreo_calidad
  GROUP BY id_revision
  ORDER BY MAX(fecha_utc) DESC, id_revision DESC
  LIMIT 1
)
SELECT tipo, nombre, valor, estado, version_indicadores, version_alternativas
FROM workspace.baci_dev.monitoreo_calidad
WHERE id_revision = (SELECT id_revision FROM ultima)
ORDER BY tipo, nombre;
```

## Comprobación

CI ejecuta las reglas compartidas con Spark local y datos sintéticos. Simula
la frontera Delta para comprobar orden de persistencia y error, uso de append,
selección explícita de versiones, contexto de reintentos y rechazo de prod.
También comprueba con Spark el esquema de registros exitosos y errores con
campos nulos. CI no valida permisos, widgets reales ni almacenamiento Delta
en el workspace.

La integración se comprueba ejecutando solamente `baci_cr_monitoreo` y leyendo
la tabla: una revisión aprobada debe contener seis reglas con valor cero y
siete métricas. Ejecutarlo otra vez debe conservar la revisión anterior y
añadir otra con distinto `id_revision`. No alterar datos reales para provocar
fallas: las pruebas negativas usan datos sintéticos en CI.

## Referencias

- [Referencias dinámicas de Jobs](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).
- [Historial y lecturas por versión](https://docs.databricks.com/aws/en/tables/history).
- [Escritura append en Delta](https://docs.databricks.com/aws/en/delta/tutorial).
