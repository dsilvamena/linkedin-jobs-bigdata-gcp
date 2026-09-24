"""Consulta 10: promedio de habilidades por oferta tras cruzar ambos CSV."""

import time

import polars as pl

from polars_comun import guardar, leer, lista_habilidades, ofertas_unicas


print("=== POLARS - CONSULTA 10 ===")
print("Cruce por job_link y promedio de habilidades por oferta")

inicio = time.time()
conteos = (
    leer("job_skills.csv")
    .unique(subset="job_link", keep="first")
    .select("job_link", lista_habilidades())
    .select("job_link", pl.col("habilidades").list.len().alias("cantidad_habilidades"))
)
resultado = (
    ofertas_unicas()
    .select("job_link")
    .join(conteos, on="job_link", how="left")
    .with_columns(pl.col("cantidad_habilidades").fill_null(0))
    .select(
        pl.len().alias("ofertas"),
        (pl.col("cantidad_habilidades") > 0).sum().alias("ofertas_con_habilidades"),
        pl.col("cantidad_habilidades").mean().round(2).alias("promedio_habilidades_por_oferta"),
    )
    .collect()
)
fin = time.time()

guardar(10, resultado, fin - inicio)
