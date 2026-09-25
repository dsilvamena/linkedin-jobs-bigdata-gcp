import time

from modin_comun import (
    pd,
    POSTINGS_PARQUET,
    iniciar_ray,
    cerrar_ray,
    guardar,
)


print("=== MODIN - CONSULTA 6 ===")
print("Top 10 países con más ofertas")

iniciar_ray()

inicio = time.perf_counter()

# Consulta 1 confirmó que no existen job_link duplicados.
# Para reducir memoria cargamos únicamente search_country.
ofertas = pd.read_parquet(
    POSTINGS_PARQUET,
    columns=["search_country"],
)

# Eliminar países nulos
paises = ofertas["search_country"].dropna()

# Eliminar valores vacíos
paises = paises[
    paises.str.strip() != ""
]

# Contar ofertas por país
conteos = paises.value_counts()

# Convertir a tabla
resultado = (
    conteos
    .rename_axis("search_country")
    .reset_index(name="ofertas")
)

# Ordenar de mayor a menor y obtener Top 10
resultado = (
    resultado
    .sort_values(
        ["ofertas", "search_country"],
        ascending=[False, True],
    )
    .head(10)
)

segundos = time.perf_counter() - inicio

guardar(
    6,
    resultado,
    segundos,
)

cerrar_ray()