"""Consulta 8: habilidades más frecuentes en las ofertas."""

import time

import polars as pl

from polars_comun import guardar, leer, lista_habilidades


print("=== POLARS - CONSULTA 8 ===")
print("Habilidades más frecuentes (explode + conteo)")

inicio = time.time()
resultado = (
    leer("job_skills.csv")
    .unique(subset="job_link", keep="first")
    .select(lista_habilidades())
    .explode("habilidades", empty_as_null=True)
    .with_columns(pl.col("habilidades").str.strip_chars().alias("habilidad"))
    .filter(pl.col("habilidad").is_not_null() & (pl.col("habilidad") != ""))
    .group_by("habilidad")
    .agg(pl.len().alias("apariciones"))
    .sort(["apariciones", "habilidad"], descending=[True, False])
    .head(20)
    .collect()
)
fin = time.time()

guardar(8, resultado, fin - inicio)
