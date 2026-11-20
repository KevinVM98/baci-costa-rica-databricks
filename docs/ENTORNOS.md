# Entornos de datos del proyecto BACI

El destino `dev` del bundle entrega el parámetro `entorno=dev` a las cuatro
tareas. Esto separa sus tablas de las que consulta el dashboard publicado.

| Uso | Entorno | Tablas | Archivos originales |
|---|---|---|---|
| Pruebas del pipeline | `dev` | `workspace.baci_dev` | Lectura de `workspace.baci.raw`. |
| Resultados del dashboard | `prod` explícito | `workspace.baci` | La etapa 01 puede preparar los archivos de `workspace.baci.raw`. |

Se conservan los nombres `comercio_enriquecido`,
`concentracion_importaciones_cr` y `alternativas_importaciones_cr` en cada
esquema. El período sigue siendo 2016–2023, HS92, edición 202501.

## Cómo llega el entorno al código

`resources/baci_cr_pipeline.job.yml` define un parámetro de Job con el valor
`${bundle.target}`. Databricks lo entrega a las tareas de notebook. Los cinco
notebooks de trabajo leen su widget `entorno` antes de ejecutar
`%run ./00_configuracion`. La variable de Python `BACI_ENTORNO` transporta el
valor a la configuración compartida; el notebook 00 no crea widgets propios.
Esto evita depender de la herencia de widgets a través de `%run`.

Sin parámetro se utiliza `dev`. Un entorno vacío o desconocido produce un error
antes de leer o escribir datos. `guardar_delta` acepta únicamente las tres
tablas del esquema elegido y crea ese esquema si no existe al guardar la
primera tabla. No se aceptan nombres arbitrarios de esquema como parámetros.

El único target del bundle sigue siendo `dev`; esta etapa no añade un despliegue
`prod`. El valor `prod` del notebook permite seleccionar explícitamente las
tablas existentes al ejecutar notebooks o configurar un Job que las mantenga.
Los parámetros pueden ser cambiados por quien tenga permiso para ejecutar con
otros valores; esta separación de nombres no sustituye los permisos de Unity
Catalog ni constituye aislamiento entre usuarios.

## Primera comprobación en el workspace existente

Antes de ejecutar desarrollo, guardá la última versión Delta de las tablas
publicadas desde el editor SQL:

```sql
DESCRIBE HISTORY workspace.baci.comercio_enriquecido LIMIT 1;
DESCRIBE HISTORY workspace.baci.concentracion_importaciones_cr LIMIT 1;
DESCRIBE HISTORY workspace.baci.alternativas_importaciones_cr LIMIT 1;
```

Desde la carpeta local que contiene `databricks.yml`, con el perfil `BACI`:

```powershell
databricks bundle validate -t dev -p BACI
databricks bundle deploy -t dev -p BACI
databricks bundle run -t dev -p BACI baci_cr_pipeline
```

Comprobá lo siguiente en la ejecución:

1. El Job tiene el parámetro `entorno` con valor `dev`.
2. Cada tarea muestra `Entorno: dev | Tablas: workspace.baci_dev`.
3. La etapa 01 muestra 0 archivos extraídos y 11 reutilizados para el período actual.
4. Las escrituras de 02 y 03 muestran nombres completos bajo `workspace.baci_dev`.
5. La etapa 04 termina correctamente y el Job muestra `TERMINATED SUCCESS`.
6. Al repetir las tres consultas de historial, las versiones de `workspace.baci`
   coinciden con las iniciales. Durante esta comprobación no debe ejecutarse
   otro Job que mantenga esas mismas tablas.

La identidad que ejecuta el Job necesita lectura del volumen y permisos para
crear/reemplazar tablas en el esquema de desarrollo. Si el esquema no existe,
necesita permiso para crearlo en el catálogo; su propietario también puede
prepararlo antes con `CREATE SCHEMA IF NOT EXISTS workspace.baci_dev` y otorgar
los permisos correspondientes.

La ejecución crea otra copia de las tres tablas derivadas, incluida la tabla
global de comercio. Reutiliza el ZIP y los CSV existentes, pero requiere espacio
para las nuevas tablas y cómputo para reconstruirlas.

## Consultar desarrollo

Abrí `05_consultas_cr` de la copia desplegada y elegí `entorno=dev`. También podés
comprobar las tablas analíticas desde SQL:

```sql
SELECT COUNT(*) AS indicadores,
       COUNT(DISTINCT codigo_producto) AS productos,
       MIN(anio) AS primer_anio, MAX(anio) AS ultimo_anio
FROM workspace.baci_dev.concentracion_importaciones_cr;

SELECT COUNT(*) AS alternativas
FROM workspace.baci_dev.alternativas_importaciones_cr;
```

Con los mismos insumos y criterios, los resultados de referencia son 33.727
indicadores, 4.524 productos, años 2016–2023 y 2.871 filas de alternativas. Son
conteos históricos de referencia, no un resultado ya observado de esta nueva
ejecución. El dashboard publicado continúa leyendo `workspace.baci`; su botón
Refresh no consulta las tablas de desarrollo.

## Si faltan archivos originales

La etapa 01 de `dev` solo reutiliza archivos cuyo tamaño coincide con el ZIP.
Ante un archivo faltante o de tamaño distinto, falla sin extraerlo ni repararlo.
También rechaza `REEXTRAER=True`. La comprobación es por tamaño, no por checksum.

Para preparar un workspace nuevo o reparar los insumos, subí el ZIP al volumen
y ejecutá **únicamente** `01_archivos_raw` con `entorno=prod`. Esta etapa prepara
archivos y no escribe las tablas analíticas. Después volvé a `dev` para ejecutar
el pipeline. Preparar o reemplazar insumos compartidos requiere que no haya
otros Jobs leyéndolos en ese momento.

## Alcance de esta etapa

El Job manual original conserva sus notebooks anteriores hasta que se actualicen.
Si se sustituyen por estos archivos, configurá `entorno=prod` en el Job para que
siga manteniendo las tablas del dashboard. No ejecutar dos reconstrucciones
simultáneas del mismo esquema: el límite de concurrencia es por Job.

No hay promoción automática de resultados ni un cambio conjunto de las tres
tablas: cada escritura sigue siendo independiente y 04 valida después de 03.
Publicar resultados solo después de validar una versión provisional es una
mejora posterior. La evidencia de CI cubre las rutas y llamadas locales; el
despliegue, los permisos y la propagación real de parámetros se comprueban con
la ejecución anterior.

## Referencias

- [Parámetros de Job en bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/job-parameters)
- [Entrega de parámetros a las tareas](https://docs.databricks.com/aws/en/jobs/job-parameters)
- [Widgets y `%run`](https://docs.databricks.com/aws/en/notebooks/widgets)
