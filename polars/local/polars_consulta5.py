"""Consulta 5: diez empresas con más publicaciones."""

import time

import polars as pl

from polars_comun import guardar, ofertas_unicas


print("=== POLARS - CONSULTA 5 ===")
print("Top 10 empresas con más publicaciones")

inicio = time.time()
resultado = (
    ofertas_unicas()
    .filter(pl.col("company").is_not_null() & (pl.col("company").str.strip_chars() != ""))
    .group_by("company")
    .agg(pl.len().alias("ofertas"))
    .sort(["ofertas", "company"], descending=[True, False])
    .head(10)
    .collect()
)
fin = time.time()

guardar(5, resultado, fin - inicio)
