"""Consulta 2: contar y tratar valores nulos de las ofertas."""

import time

import polars as pl

from polars_comun import guardar, leer


print("=== POLARS - CONSULTA 2 ===")
print("Conteo y tratamiento de valores nulos por columna")

inicio = time.time()
ofertas = leer("linkedin_job_postings.csv")
columnas = ofertas.collect_schema().names()

# Un enlace ausente no identifica una oferta. El resto de campos vacíos se
# sustituyen por una etiqueta explícita para conservar el registro.
tratadas = ofertas.filter(
    pl.col("job_link").is_not_null() & (pl.col("job_link").str.strip_chars() != "")
).with_columns(
    [
        pl.when(pl.col(columna).is_null() | (pl.col(columna).str.strip_chars() == ""))
        .then(pl.lit("No especificado"))
        .otherwise(pl.col(columna))
        .alias(columna)
        for columna in columnas if columna != "job_link"
    ]
)

antes, despues = pl.collect_all([
    ofertas.select([pl.col(c).is_null().sum().alias(c) for c in columnas]),
    tratadas.select([pl.col(c).is_null().sum().alias(c) for c in columnas]),
])
filas_descartadas = (
    ofertas.select(pl.len()).collect().item()
    - tratadas.select(pl.len()).collect().item()
)
resultado = pl.DataFrame({
    "columna": columnas,
    "nulos_antes": [antes[c][0] for c in columnas],
    "nulos_despues": [despues[c][0] for c in columnas],
})
fin = time.time()

guardar(2, resultado, fin - inicio)
print("Filas sin job_link descartadas:", filas_descartadas)
