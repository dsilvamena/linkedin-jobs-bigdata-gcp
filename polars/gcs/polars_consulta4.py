"""Consulta 4: contar las ofertas remotas."""

import time

import polars as pl

from polars_comun import RESULTADOS, guardar, ofertas_unicas


print("=== POLARS - CONSULTA 4 ===")
print("Filtrar ofertas con job_type == 'Remote'")

inicio = time.time()
remotas = ofertas_unicas().filter(pl.col("job_type") == "Remote")
resultado, muestra = pl.collect_all([
    remotas.select(pl.len().alias("ofertas_remotas")),
    remotas.select("job_link", "job_title", "company", "job_type").head(5),
])
fin = time.time()

guardar(4, resultado, fin - inicio)
muestra.write_csv(RESULTADOS / "consulta4_muestra.csv")
print("\nPrimeras 5 ofertas remotas:")
print(muestra)
