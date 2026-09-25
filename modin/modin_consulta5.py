import time

from modin_comun import (
    pd,
    POSTINGS_PARQUET,
    iniciar_ray,
    cerrar_ray,
    guardar,
)


print("=== MODIN - CONSULTA 5 ===")
print("Top 10 empresas con más ofertas")

iniciar_ray()

inicio = time.perf_counter()

# Consulta 1 confirmó que no existen job_link duplicados.
# Para reducir memoria cargamos únicamente company.
ofertas = pd.read_parquet(
    POSTINGS_PARQUET,
    columns=["company"],
)

# Eliminar empresas nulas
empresas = ofertas["company"].dropna()

# Eliminar valores vacíos
empresas = empresas[
    empresas.str.strip() != ""
]

# Contar ofertas por empresa
conteos = empresas.value_counts()

# Convertir a tabla
resultado = (
    conteos
    .rename_axis("company")
    .reset_index(name="ofertas")
)

# Ordenar y obtener Top 10
resultado = (
    resultado
    .sort_values(
        ["ofertas", "company"],
        ascending=[False, True],
    )
    .head(10)
)

segundos = time.perf_counter() - inicio

guardar(
    5,
    resultado,
    segundos,
)

cerrar_ray()