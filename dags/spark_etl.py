from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ETL with spark

# Starting Spark
spark = SparkSession.builder.appName("KayakScoring").getOrCreate()

# Connexion to PostgreSQL
jdbc_url = "jdbc:postgresql://kayak-postgres:5432/" + os.getenv("KAYAK_POSTGRES_DB")
jdbc_props = {
    "user": os.getenv("KAYAK_POSTGRES_USER"),
    "password": os.getenv("KAYAK_POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# reading weather and hotel tables from PostgreSQL
weather = spark.read.jdbc(url=jdbc_url, table="weather", properties=jdbc_props)
hotels = spark.read.jdbc(url=jdbc_url, table="hotels", properties=jdbc_props)


# Calculates the average temperature and probability of rain for the last 5 days, by city
weather_avg = weather.groupBy("city").agg(
    F.avg("temp_max").alias("avg_temp"),
    F.avg("pop").alias("avg_rain")
)

# calculation of the average city scores
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
# Writing to PostgreSQL using the JDBC protocol
result.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "city_recommendations") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite").save()


spark.stop()