from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-parquet").getOrCreate()

datos = [
    ("e1", "2026-01-15", "aprobada"),
    ("e2", "2026-01-20", "rechazada"),
    ("e3", "2026-02-03", "anotada"),
]
df = spark.createDataFrame(datos, ["event_id", "fecha", "estado"])
df = df.withColumn("mes", df.fecha.substr(1, 7))

df.write.mode("overwrite").partitionBy("mes").parquet("/datalake/test_eventos")

leido = spark.read.parquet("/datalake/test_eventos")
leido.show()
print("TOTAL:", leido.count())
spark.stop()