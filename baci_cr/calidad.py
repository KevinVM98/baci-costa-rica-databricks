"""Reglas compartidas por la validación del pipeline y el historial de calidad.

Recibe DataFrames; no lee tablas ni guarda resultados. Los nombres y la
semántica de las cuatro reglas originales de 04 se conservan.
"""

from pyspark.sql import functions as F


VERSION_REGLAS = "1"


def evaluar_calidad(
    df_indicadores, df_alternativas, *, claves, anios, clasificacion,
    version_baci, top_alternativas, codigo_cr, umbral_concentracion_pct,
    minimo_importaciones_usd,
):
    """Devolver errores (cero significa aprobado) y métricas informativas.

    Vacío y cobertura son controles binarios. Duplicados y rankings cuentan
    grupos; los otros dos controles originales cuentan filas. La ausencia
    de candidatos se informa, pero no se considera por sí sola un error.
    """
    resumen = df_indicadores.agg(
        F.count("*").alias("filas_indicadores"),
        F.countDistinct("codigo_producto").alias("productos_en_el_periodo"),
        F.countDistinct("anio").alias("anios"),
        F.min("anio").alias("anio_minimo"),
        F.max("anio").alias("anio_maximo"),
    ).first().asDict()
    cobertura = df_indicadores.select("anio", "clasificacion", "version_baci").distinct().collect()
    errores = {
        "indicadores_vacios": int(resumen["filas_indicadores"] == 0),
        "cobertura_incorrecta": int(
            {r["anio"] for r in cobertura} != set(anios)
            or any(r["clasificacion"] != clasificacion or r["version_baci"] != version_baci
                   for r in cobertura)
        ),
    }
    errores["indicadores_duplicados"] = (
        df_indicadores.groupBy(*claves).count().filter("count > 1").count()
    )
    rankings = df_alternativas.groupBy(*claves).agg(
        F.count("*").alias("filas"),
        F.countDistinct("codigo_exportador_alternativo").alias("paises"),
        F.countDistinct("posicion_alternativa").alias("posiciones"),
        F.min("posicion_alternativa").alias("posicion_minima"),
        F.max("posicion_alternativa").alias("posicion_maxima"),
    )
    errores["rankings_invalidos"] = rankings.filter(
        (F.col("filas") > top_alternativas)
        | (F.col("paises") != F.col("filas"))
        | (F.col("posiciones") != F.col("filas"))
        | (F.col("posicion_minima") != 1)
        | (F.col("posicion_maxima") != F.col("filas"))
    ).count()
    errores["filas_con_paises_o_valores_invalidos"] = df_alternativas.filter(
        (F.col("codigo_exportador_alternativo") == codigo_cr)
        | (F.col("codigo_exportador_alternativo") == F.col("codigo_exportador_principal"))
        | F.col("exportaciones_mundiales_usd").isNull()
        | (F.col("exportaciones_mundiales_usd") <= 0)
        | F.col("exportaciones_a_cr_usd").isNull()
        | (F.col("exportaciones_a_cr_usd") < 0)
        | (F.col("exportaciones_a_cr_usd") > F.col("exportaciones_mundiales_usd"))
        | F.col("destinos").isNull() | (F.col("destinos") < 1)
    ).count()
    objetivos = df_indicadores.filter(
        (F.col("participacion_principal_pct") >= umbral_concentracion_pct)
        & (F.col("total_importado_usd") >= minimo_importaciones_usd)
    )
    claves_principal = list(claves) + ["codigo_exportador_principal"]
    errores["alternativas_sin_caso_valido"] = df_alternativas.join(
        objetivos.select(*claves_principal), on=claves_principal, how="left_anti"
    ).count()
    resumen["filas_alternativas"] = df_alternativas.count()
    resumen["casos_sin_candidato"] = (
        objetivos.select(*claves).distinct()
        .join(df_alternativas.select(*claves).distinct(), on=claves, how="left_anti")
        .count()
    )
    return {"errores": errores, "metricas": resumen}
