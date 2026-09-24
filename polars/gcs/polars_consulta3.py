"""Consulta 3: convertir las habilidades de texto a listas."""

import time

import polars as pl

from polars_comun import RESULTADOS, guardar, leer, lista_habilidades


print("=== POLARS - CONSULTA 3 ===")
print("Transformar job_skills de texto a lista")

inicio = time.time()
habilidades = leer("job_skills.csv").select("job_link", lista_habilidades())
resumen, muestra = pl.collect_all([
    habilidades.select(
        pl.len().alias("filas"),
        pl.col("habilidades").list.len().sum().alias("habilidades_totales"),
    ),
    habilidades.head(5),
])
fin = time.time()

guardar(3, resumen, fin - inicio)
RESULTADOS.joinpath("consulta3_muestra.json").write_text(
    muestra.write_json(), encoding="utf-8"
)
print("\nPrimeras 5 listas:")
print(muestra)
