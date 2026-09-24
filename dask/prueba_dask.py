from dask.distributed import Client
import dask.array as da

def main():
    # Conexión al scheduler Dask
    client = Client("tcp://10.138.0.13:8786")

    print("Conectado a Dask")
    print(client)

    # Mostrar workers disponibles
    workers = client.scheduler_info()["workers"]
    print(f"\nWorkers disponibles: {len(workers)}")

    for worker in workers:
        print(f" - {worker}")

    # Crear una matriz grande dividida en bloques
    print("\nCreando matriz distribuida...")

    x = da.random.random(
        (20000, 20000),
        chunks=(2000, 2000)
    )

    # Cálculo distribuido
    print("Calculando promedio...")

    resultado = x.mean().compute()

    print("\nResultado:")
    print(resultado)

    client.close()


if __name__ == "__main__":
    main()