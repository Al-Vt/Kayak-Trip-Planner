from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from sqlalchemy import create_engine
import pandas as pd
import os
import requests
import time
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# We list all the cities selected
cities = [
    "Mont Saint Michel", "St Malo", "Bayeux", "Le Havre", "Rouen",
    "Paris", "Amiens", "Lille", "Strasbourg", "Chateau du Haut Koenigsbourg",
    "Colmar", "Eguisheim", "Besancon", "Dijon", "Annecy",
    "Grenoble", "Lyon", "Gorges du Verdon", "Bormes les Mimosas", "Cassis",
    "Marseille", "Aix en Provence", "Avignon", "Uzes", "Nimes",
    "Aigues Mortes", "Saintes Maries de la mer", "Collioure", "Carcassonne",
    "Ariege", "Toulouse", "Montauban", "Biarritz", "Bayonne", "La Rochelle"
]

BUCKET_NAME = "kayak-av-jedha"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Retrieving GPS data via the OpenStreetMap API
def get_coordinates_all_cities(**context):
    results = []
    # For each city, a request is sent to Nominatim
    for city in cities:
        headers = {"User-Agent": "kayak-trip-planner/1.0"}
        params = {"q": city + ", France", "format": "json", "limit": 1}
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            headers=headers,
            params=params
        )
        data = response.json()
        # If the city is found, the data is stored in dictionary list
        if data:
            results.append({
                "city": city,
                "latitude": float(data[0]["lat"]),
                "longitude": float(data[0]["lon"])
            })
        # Otherwise, we store None to avoid crashing the loop    
        else:
            results.append({"city": city, "latitude": None, "longitude": None})
        time.sleep(1)
    
    # We convert the dictionary list to CSV
    df_cities = pd.DataFrame(results)
    df_cities.to_csv("/opt/airflow/data/cities.csv", index=False)
    print(f"{len(df_cities)} cities saved")



# function to retrieve the weather for each city using OpenWeatherMap
def get_weather_all_cities(**context):
    
    # We take cities
    df_cities = pd.read_csv("/opt/airflow/data/cities.csv")
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    
    all_weather = []
    
    # We retrieve the city names from the CSV created with the OpenStreetMap API
    for _, row in df_cities.iterrows():
        response = requests.get(
            "https://api.openweathermap.org/data/2.5/forecast",
            params={
                "lat": row["latitude"],
                "lon": row["longitude"],
                "units": "metric",
                "appid": API_KEY
            }
        )
        data = response.json()
        
        # We create dicntionaries with the API data 
        rows = []
        for entry in data["list"]:
            rows.append({
                "city": row["city"],
                "date": entry["dt_txt"][:10],
                "temp": entry["main"]["temp"],
                "humidity": entry["main"]["humidity"],
                "pop": entry["pop"],
                "description": entry["weather"][0]["description"]
            })
        
        # Converts the dictionary list into a dataframe
        # The API returns multiple entries per day
        # Therefore, we aggregate them to obtain a single line per city per day
        df = pd.DataFrame(rows)
        df_daily = df.groupby(["city", "date"]).agg(
            temp_min=("temp", "min"),
            temp_max=("temp", "max"),
            humidity=("humidity", "mean"),
            pop=("pop", "max"),
            description=("description", "first")
        ).reset_index()
        
        all_weather.append(df_daily)
    
    # Saving
    df_weather = pd.concat(all_weather, ignore_index=True)
    df_weather.to_csv("/opt/airflow/data/weather_raw.csv", index=False)
    print(f"{len(df_weather)} saved weather rows")


# function to save weather data in the S3
def upload_to_s3(**context):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    
    BUCKET_NAME = "kayak-av-jedha"
    
    s3.upload_file(
        "/opt/airflow/data/weather_raw.csv",
        BUCKET_NAME,
        "weather_raw.csv"
    )
    print("weather_raw.csv uploaded on S3")

# ETL Function to read CSV files from the S3 and load the data into PostgreSQL
def etl_to_postgres(**context):
    # Connexion S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    
    BUCKET_NAME = "kayak-av-jedha"
    
    # Download the S3 file to the local disk of the Airflow container
    s3.download_file(BUCKET_NAME, "weather_raw.csv", "/opt/airflow/data/weather_raw.csv")
    s3.download_file(BUCKET_NAME, "hotels_raw.csv", "/opt/airflow/data/hotels_raw.csv")
    print("CSV downloaded from the S3")
    
    # Then we load the weather and hotel CSV files into PostgreSQL
    # read csv
    df_weather = pd.read_csv("/opt/airflow/data/weather_raw.csv")
    df_hotels = pd.read_csv("/opt/airflow/data/hotels_raw.csv")
    df_cities = pd.read_csv("/opt/airflow/data/cities.csv")

    # Connexion to PostgreSQL
    engine = create_engine(os.getenv("KAYAK_DB_CONN"))
    
    # Loading on PostgreSQL
    df_weather.to_sql("weather", engine, if_exists="replace", index=False)
    print(f"{len(df_weather)} weather row uploaded")
    
    df_hotels.to_sql("hotels", engine, if_exists="replace", index=False)
    print(f"{len(df_hotels)} hotel rows uploaded")

    df_cities.to_sql("cities", engine, if_exists="replace", index=False)
    print(f"{len(df_cities)} city rows uploaded")



with DAG(
    dag_id="kayak_pipeline",
    default_args=default_args,
    description="Scrape weather and hotels data for Kayak",
    schedule_interval="@weekly",  # une fois par semaine
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:




    # Geolocation
    get_coordinates_task = PythonOperator(
        task_id="get_coordinates",
        python_callable=get_coordinates_all_cities
    )

    # Weather
    get_weather_task = PythonOperator(
        task_id="get_weather",
        python_callable=get_weather_all_cities
    )

    # Upload S3
    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    # ETL
    etl_task = PythonOperator(
        task_id="etl_to_postgres",
        python_callable=etl_to_postgres
    )

    # Spark 
    spark_task = SparkSubmitOperator(
        task_id="spark_etl",
        application="/opt/airflow/dags/spark_etl.py",
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/extra/postgresql-42.7.3.jar",  # Java driver
        dag=dag
    )


    # execution order
    get_coordinates_task >> get_weather_task
    get_weather_task >> upload_s3_task
    upload_s3_task >> etl_task
    etl_task >> spark_task