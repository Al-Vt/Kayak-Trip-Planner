from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Starting Spark
spark = SparkSession.builder.appName("KayakScoring").getOrCreate()

weather = spark.read.csv("/opt/airflow/data/weather_raw.csv", header=True, inferSchema=True)
hotels = spark.read.csv("/opt/airflow/data/hotels_raw.csv", header=True, inferSchema=True)

# Mean by cities
weather_avg = weather.groupBy("city").agg(
    F.avg("temp_max").alias("avg_temp"),
    F.avg("pop").alias("avg_rain")
)
hotels_avg = hotels.groupBy("city").agg(
    F.avg("score").alias("avg_score")
)

# Join and score
# Rain multiplied by 10 to give it real weight
# Temperature and hotel: positive because the higher the score, the better
# and negative rain because the more it rains, the worse it is
result = weather_avg.join(hotels_avg, on="city") \
    .withColumn("score", F.col("avg_temp") - F.col("avg_rain") * 10 + F.col("avg_score")) \
    .orderBy(F.col("score").desc())

# Saving in PostgreSQL
result.write.format("jdbc") \
    .option("url", "jdbc:postgresql://kayak-postgres:5432/kayak_dw") \
    .option("dbtable", "city_recommendations") \
    .option("user", os.getenv("KAYAK_POSTGRES_USER")) \
    .option("password", os.getenv("KAYAK_POSTGRES_PASSWORD")) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite").save()

spark.stop()