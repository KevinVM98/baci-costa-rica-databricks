# Pruebas automáticas del proyecto BACI

Los notebooks `02` y `03` importan funciones de `catalogos.py` y
`transformaciones.py`. Las pruebas usan esas mismas funciones y comparan sus
resultados con ejemplos calculados a mano. La lectura de CSV, los controles
de entrada y las escrituras Delta permanecen en los notebooks.

## Cobertura

La suite contiene 76 casos: 30 de catálogos y transformaciones, 37 de entornos (incluidos bloqueos dev-only) y 9 de comparación dev/prod.
Los flujos e importes de prueba son sintéticos;
no constituyen nuevos resultados económicos. Los cuatro nombres con
codificación dañada reproducen el problema observado en el catálogo BACI.

| Área | Casos cubiertos |
|---|---|
| Nombres de países | Reparación, conservación de nombres válidos, repetición de la limpieza sin alterar el resultado y rechazo de texto que requiere revisión. |
| Enriquecimiento | Cantidad de filas, nombres de exportadores e importadores, cero inicial del producto, toneladas nulas y conversión a USD una sola vez. |
| Concentración | Agregación por proveedor, importador CR, separación por año, producto y edición, desempate por código y total cero. |
| Umbrales | Límites inclusivos de 90 % y US$1 millón, valores inmediatamente inferiores sin redondear, parámetros alternativos y nulos. |
| Alternativas | Exclusión de CR y del principal, exportaciones positivas, límite configurable, desempates, ventas previas a CR y ausencia de candidatos. |
| Entornos | Desarrollo por defecto, bloqueo explícito de prod, widget solo dev, insumos compartidos y guardas de escritura aunque se modifiquen variables de configuración. |
| Archivos originales | El bundle reutiliza archivos sin modificarlos; rechaza insumos faltantes, tamaños distintos, la reextracción y la activación de prod. |
| Auditoría dev/prod | Detecta valores diferentes con conteos iguales, filas duplicadas, nulos y diferencias de esquema; respeta un máximo de ejemplos. |

## Ejecutar en GitHub Actions

El archivo `.github/workflows/tests.yml` ejecuta **Pruebas BACI** después de cada
`push`, al abrir o actualizar un `pull_request`, y mediante `workflow_dispatch`.

1. Abrir **Actions** en el repositorio.
2. Seleccionar la ejecución **Pruebas BACI** correspondiente al commit.
3. Abrir **Python y PySpark** y revisar el paso **Ejecutar pruebas**.

El resultado esperado es `76 passed` y estado verde. Los registros de Actions
son la evidencia de cada ejecución y se vinculan al commit comprobado.

GitHub prepara un ejecutor Ubuntu con Python 3.12 y Java 21. Las dependencias
directas de pruebas están fijadas en `requirements-dev.txt`: pytest 8.4.2 y
PySpark 4.0.1. Esta es la versión de Spark usada para CI; no pretende identificar
la versión interna del cómputo serverless de Databricks.

El workflow comprueba la sintaxis de Python y ejecuta datos pequeños con Spark
local. No necesita tokens de Databricks, no descarga los archivos BACI y no
despliega ni modifica las tablas del workspace.

Una comprobación verde no configura protección de ramas por sí sola. Para
exigirla al fusionar pull requests hay que habilitar una regla del repositorio
por separado.

## Ejecutar localmente, de forma opcional

Usar Python 3.12 y Java 21; Java debe estar disponible en `PATH` y `JAVA_HOME`
debe apuntar a esa instalación. Ejecutar desde la raíz del repositorio.

En PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
```

En Linux:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

Para usar solo GitHub Actions no hace falta instalar Spark o Java en la PC.
No instalar `requirements-dev.txt` dentro de Databricks: su entorno de
ejecución ya proporciona PySpark.

## Comparación opcional dev/prod

Una vez que las tablas analíticas están disponibles en ambos entornos, `baci_cr_auditoria` compara su contenido sin volver a leer el comercio global. Las 9 pruebas nuevas ejercitan la función `comparar_contenido` con datos sintéticos. Véase [AUDITORIA.md](AUDITORIA.md) para ejecutar la comparación real en Databricks; CI no reemplaza esa ejecución.

## Comprobación posterior en Databricks

Las pruebas de entornos ejecutan el código real de configuración y preparación
de archivos con un ZIP pequeño. Sustituyen los widgets y Spark SQL por objetos
locales que registran llamadas. CI no ejecuta Unity Catalog, las escrituras
Delta ni comprueba los permisos o el comportamiento real de `%run` en Databricks.
El notebook `04_validaciones` conserva los controles de cobertura y coherencia
de los resultados guardados.

Después de revisar CI, desde una copia local que contenga los cambios:

```powershell
databricks bundle validate -t dev -p BACI
databricks bundle deploy -t dev -p BACI
databricks bundle run -t dev -p BACI baci_cr_pipeline
```

Los dos módulos nuevos deben desplegarse junto a `02` y `03`; no deben
importarse como notebooks. El bundle sincroniza los archivos de la carpeta
`baci_cr`. Copiar únicamente los notebooks produciría un error de importación.

`dev` entrega `entorno=dev` a todas las tareas y reconstruye las tablas de
`workspace.baci_dev`. Cualquier intento de forzar `entorno=prod` se rechaza
antes de consultar Spark; la rutina de escritura comprueba además los nombres
literales de las salidas permitidas. Los insumos se leen de
`workspace.baci.raw` sin modificarlos.
El dashboard publicado sigue consultando `workspace.baci`. La guía de
[entornos](ENTORNOS.md) incluye cómo verificar que sus versiones no cambiaron.

Comprobar `TERMINATED SUCCESS`, los mensajes de destino `workspace.baci_dev`
y la aprobación de `04_validaciones`. Consultar las tablas de desarrollo con
`05_consultas_cr` y `entorno=dev`; refrescar el dashboard no muestra estas tablas.
Para la fuente y parámetros
actuales se observaron 33 727 productos-año, 957 casos seleccionados y 2 871
filas de alternativas. Estos conteos pueden cambiar al modificar la edición,
los archivos, los umbrales o el período.

## Referencias

- [Pruebas de notebooks en Databricks](https://docs.databricks.com/aws/en/notebooks/testing)
- [Módulos Python en el workspace](https://docs.databricks.com/aws/en/files/workspace-modules)
- [Instalación de PySpark 4.0.1](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html)
- [Pruebas de Python en GitHub Actions](https://docs.github.com/en/actions/tutorials/build-and-test-code/python)
