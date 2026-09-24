"""Consulta 7: longitud media de las descripciones según el nivel del puesto."""

import time

import polars as pl

from polars_comun import guardar, leer, ofertas_unicas


print("=== POLARS - CONSULTA 7 ===")
print("Longitud promedio de la descripción por job_level")

inicio = time.time()
niveles = ofertas_unicas().select("job_link", "job_level")
resultado = (
    leer("job_summary.csv")
    .select("job_link", "job_summary")
    .join(niveles, on="job_link", how="inner")
    .filter(
        pl.col("job_summary").is_not_null()
        & pl.col("job_level").is_not_null()
        & (pl.col("job_level").str.strip_chars() != "")
    )
    .with_columns(pl.col("job_summary").str.len_chars().alias("longitud_caracteres"))
    .group_by("job_level")
    .agg(
        pl.len().alias("descripciones"),
        pl.col("longitud_caracteres").mean().round(2).alias("promedio_caracteres"),
    )
    .sort("job_level")
    .collect()
)
fin = time.time()

guardar(7, resultado, fin - inicio)
