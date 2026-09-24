"""Consulta 9: ranking completo de empresas por número de ofertas."""

import time

import polars as pl

from polars_comun import guardar, ofertas_unicas


print("=== POLARS - CONSULTA 9 ===")
print("Ordenar empresas por cantidad de ofertas (descendente)")

inicio = time.time()
resultado = (
    ofertas_unicas()
    .filter(pl.col("company").is_not_null() & (pl.col("company").str.strip_chars() != ""))
    .group_by("company")
    .agg(pl.len().alias("ofertas"))
    .sort(["ofertas", "company"], descending=[True, False])
    .collect()
)
fin = time.time()

guardar(9, resultado, fin - inicio)
