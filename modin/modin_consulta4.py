import time

from modin_comun import (
    pd,
    iniciar_ray,
    cerrar_ray,
    ofertas_unicas,
    guardar,
    RESULTADOS,
)


print("=== MODIN - CONSULTA 4 ===")
print("Filtrar ofertas de trabajo remotas")

iniciar_ray()

inicio = time.perf_counter()

# Solo cargamos job_link y job_type
ofertas = ofertas_unicas(
    ["job_type"]
)

# Filtrar ofertas remotas
remotas = ofertas[
    ofertas["job_type"] == "Remote"
]

# Cantidad total
cantidad = len(remotas)

resultado = pd.DataFrame([{
    "ofertas_remotas": cantidad
}])

# Solo 5 registros como muestra
muestra = remotas[
    ["job_link", "job_type"]
].head(5)

segundos = time.perf_counter() - inicio

# Guardar resultado principal y tiempo
guardar(
    4,
    resultado,
    segundos,
)

# Guardar muestra
RESULTADOS.mkdir(exist_ok=True)

muestra.to_csv(
    RESULTADOS / "consulta4_muestra.csv",
    index=False,
)

print("\nPrimeras 5 ofertas remotas:")
print(muestra)

cerrar_ray()