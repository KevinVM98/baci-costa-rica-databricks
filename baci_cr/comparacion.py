"""Comparar tablas analíticas de dev y prod sin escribir ni recalcular BACI.

Usa EXCEPT ALL en ambos sentidos para conservar la multiplicidad de filas:
COUNT(*) y EXCEPT DISTINCT no detectan todas las discrepancias.
"""

def comparar_contenido(df_dev, df_prod, max_ejemplos=3):
    """Devolver diferencias de contenido y una muestra acotada por cada lado.

    Requiere columnas y tipos iguales; admite distinto orden de columnas.
    No crea tablas ni modifica los DataFrames originales.
    """
    if not isinstance(max_ejemplos, int) or max_ejemplos < 0:
        raise ValueError("max_ejemplos debe ser un entero no negativo.")

    columnas_dev = df_dev.columns
    columnas_prod = df_prod.columns
    if len(set(columnas_dev)) != len(columnas_dev) or len(set(columnas_prod)) != len(columnas_prod):
        raise ValueError("Las columnas no pueden tener nombres duplicados.")
    if set(columnas_dev) != set(columnas_prod):
        faltan_en_dev = sorted(set(columnas_prod) - set(columnas_dev))
        faltan_en_prod = sorted(set(columnas_dev) - set(columnas_prod))
        raise ValueError(
            f"Esquemas incompatibles: faltan en dev={faltan_en_dev}; "
            f"faltan en prod={faltan_en_prod}."
        )

    tipos_dev = {campo.name: campo.dataType.simpleString() for campo in df_dev.schema.fields}
    tipos_prod = {campo.name: campo.dataType.simpleString() for campo in df_prod.schema.fields}
    if tipos_dev != tipos_prod:
        distintas = sorted(
            nombre for nombre in columnas_dev if tipos_dev[nombre] != tipos_prod[nombre]
        )
        detalle = {nombre: (tipos_dev[nombre], tipos_prod[nombre]) for nombre in distintas}
        raise ValueError(f"Tipos incompatibles (dev, prod): {detalle}.")

    # EXCEPT ALL usa la posición de las columnas; ordenar evita falsos positivos.
    columnas = sorted(columnas_dev)
    dev = df_dev.select(*columnas)
    prod = df_prod.select(*columnas)
    solo_dev = dev.exceptAll(prod)
    solo_prod = prod.exceptAll(dev)

    filas_solo_dev = solo_dev.count()
    filas_solo_prod = solo_prod.count()
    return {
        "filas_solo_dev": filas_solo_dev,
        "filas_solo_prod": filas_solo_prod,
        "iguales": filas_solo_dev == 0 and filas_solo_prod == 0,
        "ejemplos_solo_dev": (
            [fila.asDict(recursive=True) for fila in solo_dev.limit(max_ejemplos).collect()]
            if filas_solo_dev and max_ejemplos else []
        ),
        "ejemplos_solo_prod": (
            [fila.asDict(recursive=True) for fila in solo_prod.limit(max_ejemplos).collect()]
            if filas_solo_prod and max_ejemplos else []
        ),
    }
