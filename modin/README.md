# Consulta 7: version restaurada por lotes

Se ha revertido la lectura directa del texto completo con Modin/Ray.
`modin_consulta7.py` vuelve a leer las partes una a una con PyArrow, en lotes
 de 2.000 filas. Conserva solo enlaces y longitudes, y usa Modin para el cruce
 y la agrupacion. No requiere `modin_resumen.py`.

Actualiza `modin_consulta7.py` y `modin_comun.py` en la carpeta `modin/` del
cluster. Deten la ejecucion anterior con Ctrl+C antes de volver a ejecutar:

```bash
python3 -u modin_consulta7.py
```

Esta version corresponde a la anterior al cambio a lectura directa con Ray.
El archivo `modin_consulta7_streaming.py` conserva una version aun mas antigua.
Las salidas siguen siendo `resultados/consulta7.csv` y `consulta7_tiempo.txt`.

La lectura secuencial es mas conservadora con el texto, pero acumula enlaces
 y longitudes. No garantiza un tiempo ni un limite total de RAM. El tiempo
 incluye lectura, conversion, cruce y materializacion; excluye arranque de Ray
 y escritura. No debe compararse sin ajustes con Spark, que reutiliza postings
 cacheados de la consulta 1 y aplica filtros diferentes.

Las pruebas usan un Ray local de dos CPUs, datos pequenos y Parquet local:

```bash
python -m unittest discover -s modin -p "test_*.py" -v
```

No representan una prueba de capacidad de memoria del cluster GCP.

## Consultas 8, 9 y 10

Se siguen las consultas de Dask y Polars del repositorio:

- **8:** top 20 habilidades por apariciones, con desempate por nombre.
  Lee skills por lotes de 10.000 filas, deduplica enlaces entre todos los lotes,
  separa y cuenta con PyArrow en tareas Ray (1 CPU por tarea, maximo 2 lotes
  en curso), suma conteos en el master y selecciona candidatos con `nlargest`
  de Modin, conservando empates. Ordena esos candidatos por frecuencia y nombre
  con pandas antes de tomar los 20 finales. No recorta el top por
  lote, porque eso podria perder habilidades frecuentes en el conjunto.
- **9:** ranking completo de empresas por ofertas. Lee solo `job_link` y
  `company`, deduplica ofertas, excluye empresas nulas/vacias y ordena.
- **10:** promedio de habilidades sobre TODAS las ofertas unicas. Cuenta
  separadores `, ` sin construir listas; reduce el texto a enlaces y numeros
  antes del left join. Ofertas sin habilidades cuentan como cero.

Para ejecutar las nuevas consultas, copia **los tres scripts y el actualizado
`modin_comun.py`** a la carpeta `modin/` del cluster. No cambies consulta 7.
Desde esa carpeta ejecuta cada comando por separado, esperando que termine:

```bash
python3 -u modin_consulta8.py
python3 -u modin_consulta9.py
python3 -u modin_consulta10.py
```

Generan `resultados/consultaN.csv` y `consultaN_tiempo.txt`. Los tiempos incluyen
lectura, deduplicacion, procesamiento y materializacion; excluyen conexion
inicial a Ray y escritura. C8 y C10 son implementaciones hibridas Arrow/Modin:
la lectura por lotes y la deduplicacion global ocurren en el master. C8 tambien
combina los conteos reducidos en el master, delega la seleccion a Modin y
resuelve empates con pandas. No son benchmarks de una
ejecucion integramente distribuida.

La memoria del texto se limita por lote, pero el conjunto de enlaces vistos
crece con los enlaces unicos; C8 conserva el vocabulario y C10 los conteos de
cada enlace. No se ha medido su memoria ni rendimiento en GCP.

Las pruebas `test_consultas_finales.py` verifican los resultados con Modin/Ray
real y Parquet local, incluida la ejecucion de los tres `main` y sus salidas.
La separacion exacta `, ` y el tratamiento de blancos se conservan como en
Dask/Polars. En C10, `Go, , Rust` cuenta tres elementos, como sus listas.

Si un enlace tiene registros duplicados contradictorios, se conserva el primero
en el orden de archivos Parquet ordenados y sus filas. Ese orden puede diferir
del CSV original; para comparar motores deben coincidir los registros retenidos.
Dask corta el top 20 antes del desempate alfabetico; en un empate en el corte
puede seleccionar habilidades distintas. Esta C8 sigue el desempate de Polars.
Spark tiene diferencias adicionales: C8 devuelve top 10 y C10 agrupa por tipo
de empleo con inner join; no se han copiado esas variantes.

### Correccion de consulta 8 tras las interrupciones

Se reemplazo `claves.isin(vistos)` por busquedas individuales en el conjunto:
ya no se reconstruye una estructura con todos los enlaces anteriores en cada
lote. El conjunto sigue ocupando memoria proporcional a los enlaces unicos.
La consulta 8 evita las rutas internas de Modin de `fillna` y `value_counts`
que generaban avisos de pandas (`downcast`, `axis`, agrupacion categorica).
Tambien evita el `sort_values` distribuido de Modin, que usa una agrupacion
interna con otra llamada obsoleta en la version probada. `nlargest(keep="all")`
preserva los candidatos empatados; pandas resuelve el orden final.
No se silencian las advertencias ni se cambian las dependencias del cluster.
El trabajo textual se ejecuta con las APIs de PyArrow `split_pattern`,
`list_flatten`, `utf8_trim` y `value_counts` dentro de tareas Ray. Solo se
envian textos por lote, nunca todos los textos juntos ni enlaces a esas tareas.
El conjunto de blancos reproduce `str.strip()` de Python para conservar las reglas.

Actualizar **modin_consulta8.py y modin_comun.py** en el master. Los workers
necesitan PyArrow, ya usado por el proyecto, con una version compatible con el
entorno del master. Ray serializa la funcion de conteo; no requiere copiar el
script a los workers. Esta correccion no demuestra quien envio los SIGTERM
anteriores ni garantiza un limite de memoria para el vocabulario global.

Validacion local: Modin 0.37.1, pandas 2.3.3, Ray 2.58.0 y PyArrow 25.0.1.
Las tres pruebas especificas de C8 pasan con `PYTHONWARNINGS=error::FutureWarning`
antes de iniciar Ray. Eso comprueba esta ruta con esas versiones; C7, C9 y C10
pueden seguir produciendo avisos internos de Modin. No se indica actualizar
paquetes sueltos del cluster compartido.

Referencia: [funciones compute de Apache Arrow](https://arrow.apache.org/docs/python/api/compute.html).
