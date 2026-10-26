from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("AnalisisNotasEstudiantes") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos de los estudiantes
schema = StructType([
    StructField("estudiante_id", IntegerType()),
    StructField("materia", StringType()),
    StructField("nota", FloatType()),
    StructField("timestamp", TimestampType())
])

# Lectura del streaming desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "notas_estudiantes") \
    .load()

# Decodificación del JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Promedio de notas por estudiante en ventana de 1 minuto
promedio_notas = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "estudiante_id") \
    .agg({"nota": "avg"})

# Salida hacia la consola
query = promedio_notas.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
