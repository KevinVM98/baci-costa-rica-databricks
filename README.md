# BACI: concentración de importaciones de Costa Rica

Pipeline batch con PySpark, Databricks, Delta y Lakeflow Jobs. Procesa comercio bilateral mundial de BACI HS92, edición **202501**, para **2016–2023**. Identifica productos que Costa Rica importa con alta concentración en un país y encuentra otros países que exportaban esos mismos productos al mundo.

El Job configurado en la interfaz terminó con estado **Succeeded** el **18 de noviembre de 2026**, con una duración observada de **8 min 29 s**. Ese mismo día también se validó, desplegó y ejecutó la configuración portátil mediante **Databricks CLI v1.26.0**: el Job del destino `dev` terminó con estado **`TERMINATED SUCCESS`**. Las pruebas se realizaron en el workspace del proyecto, con los datos de origen ya disponibles; véase [el registro de validación](docs/VALIDACION.md).

La configuración separa las tablas de desarrollo (`workspace.baci_dev`) de las publicadas (`workspace.baci`). **Este bundle solo puede escribir en desarrollo**, aunque alguien intente forzar `entorno=prod`. Como ambos esquemas pertenecen a la misma cuenta, esto previene errores del pipeline pero no sustituye permisos separados de Unity Catalog. [Protección y límites](docs/ENTORNOS.md).

## Archivos del proyecto

| Archivo | Función |
|---|---|
| `databricks.yml` | Define el bundle, el destino `dev` y la carpeta de despliegue del usuario autenticado. |
| `resources/baci_cr_pipeline.job.yml` | Define las cuatro tareas del Job, sus dependencias y su configuración. |
| `resources/baci_cr_auditoria.job.yml` | Job separado para comparar desarrollo y producción sin reconstruir tablas. |
| `baci_cr/00_configuracion.py` | Contiene período, edición, rutas y filtros analíticos compartidos. |
| `baci_cr/01_archivos_raw.py` | Verifica los archivos raw y solo reutiliza las copias existentes; no los extrae ni modifica. |
| `baci_cr/02_comercio_enriquecido.py` | Lee los CSV, valida los catálogos, realiza las uniones y guarda el comercio global. |
| `baci_cr/03_indicadores_cr.py` | Calcula la concentración de importaciones de CR y los países alternativos. |
| `baci_cr/04_validaciones.py` | Comprueba cobertura y coherencia de las tablas analíticas. |
| `baci_cr/05_consultas_cr.py` | Consulta el resumen anual, productos seleccionados, evolución y alternativas. |
| `baci_cr/06_comparar_entornos.py` | Auditoría de solo lectura de las dos tablas analíticas de dev y prod. |
| `baci_cr/catalogos.py` | Función compartida para reparar los nombres de países. |
| `baci_cr/transformaciones.py` | Funciones PySpark de enriquecimiento, concentración y alternativas. |
| `baci_cr/comparacion.py` | Comparación de DataFrames en ambos sentidos, conservando duplicados. |
| `tests/` | Pruebas de las funciones con ejemplos pequeños y resultados conocidos. |
| `.github/workflows/tests.yml` | Ejecuta las pruebas automáticamente en GitHub Actions. |
| `docs/PRUEBAS.md` | Explica la cobertura de las pruebas, CI y la comprobación en Databricks. |
| `docs/ENTORNOS.md` | Explica la separación de tablas, los insumos compartidos y cómo verificarla. |
| `docs/AUDITORIA.md` | Cómo ejecutar e interpretar la comparación entre dev y prod. |
| `docs/VALIDACION.md` | Registra las evidencias observadas y el alcance de la revisión local. |

Los archivos numerados `00`–`05` conservan el formato de notebook fuente de Databricks. El procesamiento de los datos BACI se ejecuta en Databricks; no se ejecutan estos notebooks con `python archivo.py` en la computadora personal. `catalogos.py` y `transformaciones.py` son módulos Python normales que deben mantenerse junto a los notebooks y desplegarse con el bundle.

## Pruebas automáticas

La suite incluye 68 casos: 30 para nombres y transformaciones PySpark, 29 para la configuración de entornos y 9 para comparar datos entre desarrollo y producción con Spark local. Los notebooks `02` y `03` importan las mismas funciones que verifican las pruebas.

El workflow **Pruebas BACI** se ejecuta en cada `push` y `pull_request`, y también puede iniciarse manualmente desde GitHub Actions. Usa Python 3.12, Java 21, pytest 8.4.2 y PySpark 4.0.1 con datos sintéticos; no necesita credenciales ni cómputo de Databricks. El resultado esperado es `68 passed`.

Las pruebas complementan a `04_validaciones`, que revisa las tablas reales. La integración completa se comprueba al desplegar y ejecutar el Job en Databricks. [Consultar las instrucciones de pruebas](docs/PRUEBAS.md).

## Orden de ejecución

| Tarea | Depende de | Resultado |
|---|---|---|
| `01_archivos_raw` | — | Ocho CSV de comercio, dos catálogos y `Readme.txt`. |
| `02_comercio_enriquecido` | `01_archivos_raw` | Tabla global `comercio_enriquecido`. |
| `03_indicadores_cr` | `02_comercio_enriquecido` | Tablas `concentracion_importaciones_cr` y `alternativas_importaciones_cr`. |
| `04_validaciones` | `03_indicadores_cr` | Controles y resumen de cobertura. |

Las tareas posteriores usan `ALL_SUCCESS`. Los notebooks `01`–`05` leen el parámetro `entorno` y después cargan `00_configuracion` con `%run ./00_configuracion`. `05_consultas_cr` sirve para explorar resultados. `06_comparar_entornos` es una auditoría independiente de solo lectura, que compara las tablas analíticas dev y prod sin volver a procesar comercio global. Sin un parámetro explícito se utiliza `dev` en las etapas que lo necesitan.

## Auditoría de resultados entre dev y prod

Después de reconstruir desarrollo, se pueden comparar los resultados de las dos tablas analíticas con producción sin ejecutar las etapas 01–04. Para ello se despliega el Job independiente y se ejecuta:

```powershell
databricks bundle run -t dev -p BACI baci_cr_auditoria
```

Primero ejecutá `databricks bundle validate -t dev -p BACI` y `databricks bundle deploy -t dev -p BACI` para publicar la configuración nueva del Job. El control compara valores, esquema y duplicados en ambas direcciones, sin modificar tablas y sin leer el comercio global. Por defecto falla ante divergencias, incluso si ambos conteos son iguales. [Más detalles y límites de la auditoría](docs/AUDITORIA.md).

## Datos y preparación del workspace

El paquete contiene código y documentación. El insumo necesario es **`BACI_HS92_V202501.zip`**, que debe obtenerse de CEPII o de la copia de esa edición conservada para el proyecto. El archivo de datos no está incluido en este paquete.

Con la configuración actual, deben existir:

| Recurso | Nombre o ruta |
|---|---|
| Catálogo | `workspace` |
| Esquema | `workspace.baci` |
| Volumen | `workspace.baci.raw` |
| ZIP de origen | `/Volumes/workspace/baci/raw/BACI_HS92_V202501.zip` |

En un workspace nuevo, prepará el volumen, el ZIP y los CSV mediante un procedimiento administrativo separado **antes** de usar este bundle. El notebook `01_archivos_raw` del bundle no extrae archivos ni admite `entorno=prod`; solo verifica el tamaño y presencia de los 11 archivos esperados y falla sin modificarlos si alguno falta o no coincide.

La identidad de desarrollo necesita lectura del volumen y permisos para crear y reemplazar tablas en `workspace.baci_dev`. Si ese esquema todavía no existe, necesita permiso para crearlo en el catálogo; alternativamente, su propietario puede prepararlo antes. La preparación inicial del ZIP/CSV necesita un proceso independiente. `CATALOGO` y `ESQUEMA` están fijados a las salidas de desarrollo; `CATALOGO_RAW`, `ESQUEMA_RAW` y `VOLUMEN` indican los insumos compartidos.

La etapa 01 solo comprueba la presencia de los años configurados y los catálogos. Reutiliza un archivo cuando su tamaño coincide con el registrado en el ZIP; esa comparación no comprueba igualdad criptográfica. El ZIP sigue siendo necesario aunque los CSV estén extraídos.

## Resultados observados con los parámetros actuales

| Medida | Resultado |
|---|---:|
| Filas de comercio mundial | 86.758.220 |
| Combinaciones de producto y año para CR | 33.727 |
| Productos distintos importados por CR en el período | 4.524 |
| Años | 8, de 2016 a 2023 |
| Casos seleccionados de producto y año | 957 |
| Filas de países candidatos | 2.871 |
| Casos seleccionados sin candidatos | 0 |

Estos valores proceden de las salidas compartidas por el usuario. Los 957 casos incluyen productos repetidos en distintos años. Cambiar edición, período, filtros o archivos puede cambiar los conteos.

## Tablas y criterios

Las tres salidas del bundle son tablas Delta bajo `workspace.baci_dev`. El parámetro `entorno=prod` se rechaza antes de leer o escribir datos. Las tablas ya publicadas siguen en `workspace.baci` y el dashboard continúa consultándolas:

| Tabla | Unidad de una fila |
|---|---|
| `comercio_enriquecido` | Flujo de un exportador a un importador, para un producto y año. |
| `concentracion_importaciones_cr` | Un producto importado por Costa Rica en un año y edición. |
| `alternativas_importaciones_cr` | Un país candidato para un producto, año y edición seleccionados. |

El valor original `v` está expresado en miles de USD; `valor_usd = v × 1000`. La participación principal es el valor importado desde el país principal dividido entre el valor total importado de ese producto y año, multiplicado por 100.

Los parámetros actuales seleccionan productos con importaciones de CR **≥US$1 millón por año** y participación del exportador principal **≥90 %**. Para cada caso se conservan hasta tres países alternativos, ordenados por valor exportado mundial; el número de destinos y el código del país resuelven empates. Se excluyen Costa Rica y el exportador principal.

Los filtros son decisiones exploratorias del proyecto. Los países candidatos representan exportaciones históricas; pueden incluir reexportaciones y no acreditan oferta disponible, viabilidad logística ni empresas proveedoras. Un cero hacia CR indica ausencia de un flujo registrado en ese recorte de BACI. Los montos son nominales.

En la tabla de alternativas se repite el total importado por CR por cada candidato. Para sumar importaciones anuales se utiliza la tabla de concentración.

El catálogo de productos incluye `9999AA`, descrito por la fuente como código legado no WCO. La etapa 02 admite esa excepción en el catálogo y conserva el texto exacto. Los códigos numéricos se completan a seis dígitos antes de unir las tablas; los códigos de comercio del período probado superaron su validación numérica.

## Ejecutar el Job que ya existe

En Databricks, abrí `Jobs & Pipelines`, seleccioná `baci_cr_pipeline` y usá `Run now`. Se configuró para ejecución manual. Al finalizar, revisá el estado de la ejecución y la salida de `04_validaciones`.

El Job original creado en la interfaz y el Job del bundle son recursos distintos. El despliegue de `dev` actualiza únicamente el segundo. El Job original conserva sus notebooks y configuración anteriores; **no debe reemplazarse con los notebooks protegidos de este bundle para mantener producción**, ya que rechazan `entorno=prod`. La publicación futura requerirá una ruta separada y revisada. No ejecutar reconstrucciones simultáneas sobre el mismo esquema.

La etapa 02 reemplaza la tabla global y la 03 reemplaza las dos tablas analíticas. La carga es una reconstrucción completa, no incremental. Cada tabla se confirma por separado; los resultados se consumen después de completar las cuatro etapas.

## Validar y desplegar el paquete con Databricks CLI

Extraé el ZIP en una carpeta local y abrila en VS Code. Los comandos siguientes se ejecutan desde la carpeta que contiene `databricks.yml`.

Instalá la versión vigente de [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). Comprobá que la terminal la reconoce:

```powershell
databricks --version
```

Para iniciar sesión de forma interactiva, reemplazá `URL_REAL_DEL_WORKSPACE` por la URL base de tu workspace y utilizá el perfil local `BACI`:

```powershell
databricks auth login --host "URL_REAL_DEL_WORKSPACE" --profile BACI
```

Validá los archivos contra la configuración y el workspace autenticado:

```powershell
databricks bundle validate -t dev -p BACI
```

Cuando esa validación termine correctamente, el siguiente comando sube los notebooks y crea o actualiza los recursos administrados por este bundle:

```powershell
databricks bundle deploy -t dev -p BACI
```

El primer despliegue crea un Job de desarrollo con prefijo `[dev ...]`. El bundle no está vinculado al Job que se creó manualmente en la interfaz. Las rutas relativas de `resources/baci_cr_pipeline.job.yml` apuntan a los notebooks incluidos; el usuario y el workspace se resuelven mediante la autenticación.

Para ejecutarlo:

```powershell
databricks bundle run -t dev -p BACI baci_cr_pipeline
```

Se mantiene `PERFORMANCE_OPTIMIZED` y la cola habilitada del YAML exportado. `max_concurrent_runs: 1` limita a una la ejecución simultánea de este Job, incluso en modo `development`. El bundle entrega `entorno=${bundle.target}` a las cuatro tareas y solo admite el destino `dev`. Cualquier intento de forzar `prod` falla antes de procesar datos. Los CSV se leen del volumen compartido sin extraerlos ni reemplazarlos. [Verificar la protección de entornos](docs/ENTORNOS.md).

Para cambiar período o filtros, editá `baci_cr/00_configuracion.py`, validá y desplegá de nuevo. El entorno sí se recibe como parámetro del Job; período y filtros siguen definidos en el código. Esta versión no descarga automáticamente nuevas ediciones de BACI.

## Fuentes

- [CEPII — BACI](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37).
- [Configuración de bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/settings).
- [Referencia de rutas y configuración](https://docs.databricks.com/aws/en/dev-tools/bundles/reference).
- [Tareas de notebooks en bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types).
- [Validar, desplegar y ejecutar un Job con bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial).
- [Autenticación de Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/authentication).
- [Modo de desarrollo y concurrencia](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes).

## Dashboard interactivo

Dashboard de importaciones de Costa Rica para 2016–2023, con
evolución anual, concentración de proveedores y países exportadores
alternativos. Incluye filtros por año y código de producto.

- [Guía de uso y recuperación](docs/DASHBOARD.md)
- [Configuración exportada del dashboard](dashboards/)

![Caso verificado: producto 230330 en 2023](docs/images/caso_2023_230330.png)

Captura tomada durante la configuración y comprobación del dashboard.
