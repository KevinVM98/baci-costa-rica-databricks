# Auditoría de desarrollo contra producción

El notebook `baci_cr/06_comparar_entornos.py` y el Job `baci_cr_auditoria`
permiten comparar los resultados de desarrollo y las tablas publicadas sin
reconstruir BACI. El dashboard sigue usando `workspace.baci`.

## Qué compara

| Tabla | Desarrollo | Producción |
|---|---|---|
| Indicadores de concentración | `workspace.baci_dev.concentracion_importaciones_cr` | `workspace.baci.concentracion_importaciones_cr` |
| Alternativas por producto y año | `workspace.baci_dev.alternativas_importaciones_cr` | `workspace.baci.alternativas_importaciones_cr` |

Se comprueba que los nombres de columnas y sus tipos coincidan y después
se comparan **todas las filas** de cada tabla, aunque las columnas o las filas
tengan distinto orden. Se utiliza `EXCEPT ALL` en ambos sentidos para no perder
duplicados. Si una tabla contiene el mismo número de filas, pero uno de sus
valores es diferente, el control lo detecta. Los valores nulos se comparan
según la semántica de `EXCEPT ALL` de Spark SQL.

No se lee ni se reconstruye la tabla global `comercio_enriquecido` de más de
86 millones de filas. No se hace `CREATE`, `INSERT`, `UPDATE`, `MERGE` ni `DROP`.
Esta comparación **no** verifica exhaustivamente que los archivos raw de ambos
entornos sean iguales: verifica las tablas analíticas guardadas.

## Ejecutar en el workspace existente

Desde la carpeta local que contiene `databricks.yml`:

```powershell
databricks bundle validate -t dev -p BACI
databricks bundle deploy -t dev -p BACI
databricks bundle run -t dev -p BACI baci_cr_auditoria
```

El despliegue registra el Job separado de auditoría. El comando `bundle run`
ejecuta únicamente la tarea `06_comparar_entornos`; **no inicia las etapas
01–04** del pipeline principal.

También se puede abrir y ejecutar el notebook 06 desplegado en Databricks.
La identidad de ejecución necesita permiso de lectura sobre las cuatro tablas
analíticas. No requiere permiso para escribirlas.

## Interpretar el resultado

- `IGUAL`: no se detectaron filas exclusivas de ningún entorno.
- `DIFERENTE`: se muestran cantidades de filas exclusivas de cada entorno
  y un máximo de tres ejemplos de cada lado.
- `Esquemas incompatibles` o `Tipos incompatibles`: las columnas o sus tipos
  ya no coinciden. Se interrumpe la comparación para evitar resultados
  engañosos.

El notebook tiene `FALLAR_SI_DIFERENCIAS = True` de forma predeterminada:
termina con error si hay divergencias, lo cual sirve como control previo
a una futura publicación. Se puede cambiar a `False` para inspeccionar una
diferencia intencional sin marcar el Job como fallido.

**Una diferencia no siempre es un error**: también puede ser consecuencia
de nuevas reglas de negocio o parámetros. Antes de aprobar cambios,
revisar los ejemplos, las transformaciones y la versión de los insumos.
Una igualdad tampoco demuestra por sí sola que los datos económicos sean
correctos: las validaciones de calidad son un control separado.

Las consultas se hacen sobre el estado disponible durante la ejecución,
no sobre una instantánea transaccional conjunta de ambas tablas. Evitá
reconstruir `dev` o `prod` mientras corre la auditoría para que la comparación
sea interpretable.

## Pruebas y alcance

Las pruebas de `tests/test_comparacion.py` usan DataFrames pequeños y sintéticos
en PySpark local. Comprueban igualdad con columnas reordenadas, cambios de
valor aun con conteos iguales, multiplicidad, nulos, tipos incompatibles y
límite de ejemplos. GitHub Actions no tiene que acceder a Databricks.

Esta funcionalidad se incorpora al repositorio y a un Job **independiente**.
No cambia ni publica las tablas de producción, no añade un despliegue `prod`
ni sustituye el proceso de aprobación manual.
