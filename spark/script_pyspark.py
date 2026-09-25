import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, expr, length, size, split, trim

spark = (
    SparkSession.builder.appName("LinkedInJobs_PySpark_Benchmark").getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BUCKET = "gs://utec-linkedin-jobs-2026/processed_partitioned"
PATH_POSTINGS = f"{BUCKET}/postings_parquet"
PATH_SKILLS = f"{BUCKET}/skills_parquet"
PATH_SUMMARY = "gs://utec-linkedin-jobs-2026/processed/job_summary_parquet"

tiempos = {}

print("🚀 Iniciando Benchmark en PySpark...")

# C1: Limpieza de Duplicados
t0 = time.time()
df_postings = spark.read.parquet(PATH_POSTINGS)
df_postings_clean = df_postings.dropDuplicates(["job_link"])
df_postings_clean.cache()
df_postings_clean.count()
tiempos["Consulta 1 (Duplicados)"] = time.time() - t0

# C2: Imputación de Nulos
t0 = time.time()
df_postings_clean = df_postings_clean.fillna(
    {"company": "Not Specified", "job_location": "Not Specified"}
)
df_postings_clean.count()
tiempos["Consulta 2 (Imputación Nulos)"] = time.time() - t0

# C3: Habilidades a Lista
t0 = time.time()
df_skills = spark.read.parquet(PATH_SKILLS)
df_skills_parsed = df_skills.withColumn(
    "skills_list", split(col("job_skills"), ",")
)
df_skills_parsed.cache()
df_skills_parsed.count()
tiempos["Consulta 3 (Parsing Habilidades)"] = time.time() - t0

# C4: Filtrado Modalidad Remota
t0 = time.time()
df_remote = df_postings_clean.filter(col("job_type") == "Remote")
df_remote.count()
tiempos["Consulta 4 (Filtro Remoto)"] = time.time() - t0

# C5: Top 10 Empresas con Más Ofertas
t0 = time.time()
df_postings_clean.groupBy("company").count().orderBy(
    col("count").desc()
).limit(10).collect()
tiempos["Consulta 5 (Top Empresas)"] = time.time() - t0

# C6: Top 10 Países
t0 = time.time()
df_postings_clean.groupBy("search_country").count().orderBy(
    col("count").desc()
).limit(10).collect()
tiempos["Consulta 6 (Top Países)"] = time.time() - t0

# C7: Promedio de Longitud de Descripción por Nivel
t0 = time.time()
df_summary = spark.read.parquet(PATH_SUMMARY)
df_summary_len = df_summary.withColumn(
    "desc_length", length(col("job_summary"))
)
df_merged_summary = df_postings_clean.join(
    df_summary_len, "job_link", "inner"
)
df_merged_summary.groupBy("job_level").agg(avg("desc_length")).collect()
tiempos["Consulta 7 (Promedio Longitud Desc)"] = time.time() - t0

# C8: Top 10 Habilidades Más Demandadas
t0 = time.time()
df_skills_exploded = df_skills_parsed.select(
    "job_link", expr("explode(skills_list) as skill")
)
df_skills_clean = df_skills_exploded.withColumn("skill", trim(col("skill")))
df_skills_clean.groupBy("skill").count().orderBy(col("count").desc()).limit(
    10
).collect()
tiempos["Consulta 8 (Top Habilidades)"] = time.time() - t0

# C9: Ranking de Empresas por Volumen
t0 = time.time()
df_postings_clean.groupBy("company").agg(
    count("job_link").alias("total")
).orderBy(col("total").desc()).collect()
tiempos["Consulta 9 (Ranking Empresas)"] = time.time() - t0

# C10: Promedio de Habilidades por Tipo de Empleo
t0 = time.time()
df_skills_count = df_skills_parsed.withColumn(
    "num_skills", size(col("skills_list"))
)
df_merged_skills = df_postings_clean.join(df_skills_count, "job_link", "inner")
df_merged_skills.groupBy("job_type").agg(avg("num_skills")).collect()
tiempos["Consulta 10 (Promedio Skills por Modalidad)"] = time.time() - t0

# Impresión de Resultados
print("\n" + "=" * 50)
print("⏱️ TIEMPOS DE EJECUCIÓN EN PYSPARK")
print("=" * 50)
for consulta, duracion in tiempos.items():
  print(f"{consulta}: {duracion:.4f} segundos")
print("=" * 50)

spark.stop()
