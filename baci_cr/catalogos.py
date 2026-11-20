"""Limpieza del catálogo BACI compartida por el notebook 02 y sus pruebas."""


def normalizar_nombre_pais(nombre):
    """Revertir una capa reconocible de UTF-8 interpretado como Latin-1."""
    import re

    if nombre is None:
        return None
    if "\ufffd" in nombre:
        raise ValueError(
            f"El nombre contiene un carácter de reemplazo y requiere revisión: {nombre!r}"
        )

    # Secuencias de bytes UTF-8 representadas erróneamente como caracteres Latin-1.
    # Una tilde válida aislada (por ejemplo, ü en Türkiye) no coincide con ellas.
    patron = (
        r"[\u00c2-\u00df][\u0080-\u00bf]"
        r"|[\u00e0-\u00ef][\u0080-\u00bf]{2}"
        r"|[\u00f0-\u00f4][\u0080-\u00bf]{3}"
    )
    if not re.search(patron, nombre):
        return nombre

    try:
        corregido = nombre.encode("latin-1", errors="strict").decode(
            "utf-8", errors="strict"
        )
    except UnicodeError as error:
        raise ValueError(
            f"No se pudo reparar el nombre sin pérdida de caracteres: {nombre!r}"
        ) from error

    if "\ufffd" in corregido or re.search(patron, corregido):
        raise ValueError(
            f"El nombre sigue mostrando codificación dañada y requiere revisión: {nombre!r}"
        )
    return corregido
