# Consultas Polars

Este directorio separa las ejecuciones por origen de datos:

- `local/`: procesa los tres CSV de `data_proyecto/`.
- `gcs/`: procesa los mismos archivos desde Google Cloud Storage.

## Ejecutar localmente

Desde la raíz del repositorio:

```bash
uv sync
uv run python polars_proyecto/local/ejecutar_todas.py
```

Para una consulta individual, por ejemplo la 7:

```bash
uv run python polars_proyecto/local/polars_consulta7.py
```

Los resultados se escriben en `polars_proyecto/local/resultados/`. Cada
consulta genera `consultaN.csv` y `consultaN_tiempo.txt`; el ejecutor conjunto
además crea `tiempos_polars.csv`.

## Ejecutar desde GCS

Las instrucciones y dependencias adicionales están en
[`gcs/README.md`](gcs/README.md).
