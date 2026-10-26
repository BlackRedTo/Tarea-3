from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import urllib.request

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisAvanzado-TRM") \
    .getOrCreate()

print("--- CARGANDO EL CONJUNTO DE DATOS ---")
url = "https://www.datos.gov.co/api/v3/views/mcec-87by/query.csv"
archivo_local = "trm_datos.csv"
urllib.request.urlretrieve(url, archivo_local)

df = spark.read.option("header", "true") \
              .option("inferSchema", "true") \
              .csv(archivo_local)

# Limpieza básica de nulos
df_limpio = df.na.drop()


print("\n--- ANÁLISIS 1: COMPORTAMIENTO Y PROMEDIO ANUAL DE LA TRM ---")
# Extraemos el año de la fecha y agrupamos para calcular estadísticas por año
df_con_anio = df_limpio.withColumn("Anio", F.year(F.col("VIGENCIADESDE")))

resumen_anual = df_con_anio.groupBy("Anio") \
    .agg(
        F.round(F.avg("VALOR"), 2).alias("Promedio_TRM"),
        F.max("VALOR").alias("TRM_Maxima"),
        F.min("VALOR").alias("TRM_Minima")
    ) \
    .orderBy("Anio", ascending=False)

# Mostramos el resultado agrupado
resumen_anual.show(15, truncate=False)


print("\n--- ANÁLISIS 2: TOP 5 DÍAS CON LA TRM MÁS ALTA HISTÓRICA ---")
# Ordenamiento para hallar los picos más altos del dólar
top_historico = df_limpio.orderBy(F.col("VALOR").desc()) \
    .select("VIGENCIADESDE", "VALOR")

top_historico.show(5, truncate=False)

# Cerrar sesión
spark.stop()
