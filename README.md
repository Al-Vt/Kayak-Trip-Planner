# Kayak Trip Planner

The goal was to build an automated data pipeline to re-schedule the best destinations based on weather and hotel data.


To carry out this project, the GPS coordinates of 35 French cities were first gathered using the Noninatim API. Weather data was collected with the OpenWeatherMap API, and then data from 800 hotels was scraped from [Booking.com](http://Booking.com).
All of this data was then stored in an S3 bucket. The cleaned data was loaded into PostgreSQL.
The entire process was automated in a pipeline using Apache Airflow and containerized with Docker.
Finally, an interactive streamlit was created to allow users to sort the results according to the importance they assign to different criteria.

## Tech Stack

* **Scraping** : Selenium + BeautifulSoup + Scrapy
* **APIs** : Nominatim, OpenWeatherMap
* **Data Lake** : AWS S3
* **Data Warehouse** : PostgreSQL
* **Orchestration** : Apache Airflow
* **Visualisation** : Plotly + Streamlit
* **Containerisation** : Docker

## Architecture

<img width="2120" height="820" alt="Kayak" src="https://github.com/user-attachments/assets/b0d20683-5084-4dcb-a8b0-b399c03a38f1" />

```
Nominatim          → GPS coordinates for 35 cities
OpenWeatherMap     → 6-day weather forecast          →  S3  →  PostgreSQL  →  Streamlit
Booking.com        → 800 hotels scraped
```

## Getting Started

```bash
# Start all services
docker-compose up -d

# Access Airflow
http://localhost:8081

# Access the app
http://localhost:8501
```

## Project Structure

```
├── dags/
│   └── kayak_pipeline.py     # Airflow DAG (@weekly)
├── data/                     # Temporary data files
├── app.py                    # Streamlit application
├── booking_scraper.py        # Booking.com scraper
├── docker-compose.yml        # Docker infrastructure
├── Dockerfile.streamlit      # Streamlit image
└── .env                      # Environment variables (not versioned)
```

## Environment Variables

Create a `.env` file at the root of the project:

```
OPENWEATHER_API_KEY=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=eu-west-3
KAYAK_POSTGRES_USER=
KAYAK_POSTGRES_PASSWORD=
KAYAK_POSTGRES_DB=
```


