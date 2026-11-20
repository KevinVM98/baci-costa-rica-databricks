# Entornos de datos y protección de producción (BACI)

## Situación actual

| Recurso | Ruta | Uso desde este bundle |
|---|---|---|
| Tablas derivadas de desarrollo | `workspace.baci_dev` | Lectura y escritura de las tres tablas |
| Tablas del dashboard | `workspace.baci` | **Sin escritura** desde los notebooks 01–05 |
| ZIP y CSV originales | `/Volumes/workspace/baci/raw` | Solo lectura y verificación de tamaños |
| Historial de calidad | `workspace.baci_dev.monitoreo_calidad` | Append desde el Job independiente de monitoreo |
| Auditoría | `baci_cr_auditoria` | Lee dos tablas analíticas en cada esquema; no publica resultados |

Los dos esquemas tienen como propietario a la misma identidad personal que ejecuta los
Jobs. La revisión en Databricks del 20 de noviembre de 2026 identificó al mismo
propietario en `baci` y `baci_dev`. Por tanto, **esto es una protección en la
aplicación contra errores accidentales, no aislamiento de seguridad de Unity Catalog**.
El propietario puede seguir modificando producción por SQL u otros Jobs.

## Bloqueo preventivo del bundle de desarrollo

Este repositorio solo despliega el target `dev`. Los notebooks `01`–`05` y `07`
ofrecen exclusivamente el widget `entorno=dev` y cargan el notebook compartido
`00_configuracion` antes de leer o escribir tablas.

- Si se fuerza `entorno=prod` en un parámetro del Job o notebook,
  `00_configuracion` genera `ValueError: Producción bloqueada` antes de procesar datos.
- `guardar_delta` tiene una segunda comprobación de entorno y catálogo/esquema.
  Solo admite los nombres literales de las tres tablas bajo `workspace.baci_dev`.
  Una desviación falla antes de emitir SQL o registrar vistas temporales.
- `01_archivos_raw` se limita a comprobar que los 11 archivos usados actualmente
  están presentes y tienen tamaños consistentes con el ZIP; no reextrae.
  También rechaza `REEXTRAER=True` y un cambio sobrevenido a `prod`.
- El Job de auditoría `baci_cr_auditoria` mantiene lecturas de ambas tablas
  analíticas en `dev` y `prod`, sin escrituras.

El Job `baci_cr_monitoreo` agrega resultados al historial mediante una rutina
separada con destino literal y modo append. La lista de tres tablas permitidas
por `guardar_delta` no se amplía: esa función sigue dedicada a reconstruir las
salidas del pipeline. El monitoreo lee únicamente las dos tablas analíticas de
desarrollo. Véase [MONITOREO.md](MONITOREO.md).

El Job histórico creado manualmente en Databricks no forma parte de este bundle
y **no queda protegido automáticamente**. Tampoco se impiden consultas SQL directas
del propietario ni se cambia ninguna concesión de permisos.

## Validar y desplegar la protección

Desde la carpeta local que contiene `databricks.yml` y el perfil `BACI`:

```powershell
databricks bundle validate -t dev -p BACI
databricks bundle deploy -t dev -p BACI
```

Desplegar solo actualiza el código y los recursos del bundle; **no inicia una
reconstrucción** ni modifica las tablas Delta. Ejecutar posteriormente el Job
de monitoreo sí crea o agrega filas a su tabla de historial en desarrollo. GitHub Actions comprueba las
rutas permitidas y el rechazo del entorno de producción con datos sintéticos.

### Prueba negativa sin procesar 86,7 millones de filas

Tras el despliegue, comprobar en una ejecución aislada del notebook
`00_configuracion` que la variable `BACI_ENTORNO = "prod"`, establecida
antes de ejecutar su código, provoca un error `Producción bloqueada`.
No probar este rechazo ejecutando el Job completo: si el parámetro llegara
a omitirse por un error de configuración, se reconstruiría `dev`.
Para una comprobación sin recursos Databricks, consultar los tests
`tests/test_entornos.py` y el estado verde de GitHub Actions.

## Lo que falta para aislamiento real

Para restringir el acceso efectivo a producción habrá que diseñar una identidad
de ejecución de desarrollo sin privilegios de modificación sobre
`workspace.baci`, mantener lectura donde se necesite auditar y reservar un
flujo de publicación de producción revisado. No se cambia de dueño ni se
revocan permisos en esta etapa, ya que actualmente ambos esquemas dependen
de la misma cuenta. Evitar otorgar a la identidad de desarrollo permisos
amplios de propietario o administración.

## Comprobación de integridad y límites

Las tablas analíticas se compararon el 20 de noviembre de 2026 mediante el
Job de auditoría; el resultado observado fue cero filas exclusivas en ambos
sentidos para concentración y alternativas. Véase
[VALIDACION.md](VALIDACION.md) y [AUDITORIA.md](AUDITORIA.md).
Esto no verifica la igualdad del comercio global ni sustituye la revisión de
los permisos del workspace.

La reconstrucción del pipeline sigue escribiendo cada tabla por separado.
No hay publicación atómica ni promoción automática de dev a prod.
