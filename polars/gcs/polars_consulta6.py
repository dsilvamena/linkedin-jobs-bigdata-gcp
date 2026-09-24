"""Consulta 6: diez países con más ofertas."""

import time

import polars as pl

from polars_comun import guardar, ofertas_unicas


print("=== POLARS - CONSULTA 6 ===")
print("Top 10 países con más ofertas")

inicio = time.time()
resultado = (
    ofertas_unicas()
    .filter(pl.col("search_country").is_not_null() & (pl.col("search_country").str.strip_chars() != ""))
    .group_by("search_country")
    .agg(pl.len().alias("ofertas"))
    .sort(["ofertas", "search_country"], descending=[True, False])
    .head(10)
    .collect()
)
fin = time.time()

guardar(6, resultado, fin - inicio)
