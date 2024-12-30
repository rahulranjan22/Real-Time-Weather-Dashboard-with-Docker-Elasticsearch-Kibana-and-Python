import requests
from elasticsearch import Elasticsearch
import schedule
import time
import datetime
import os

# Elasticsearch configuration
ES_HOST = os.getenv("ES_HOST", "https://url-77a9e1.es.us-central1.gcp.cloud.es.io")
ES_USERNAME = os.getenv("ES_USERNAME", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "password")
ES_INDEX = "weather-data"

# Weather API configuration
API_KEY = os.getenv("API_KEY", "78687af7e9bb4ef19f784215242812")
CITIES = ["London", "Bangalore", "New York", "New Delhi","Pune", "Chennai", "Mumbai"]

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD)
)

# Create index if not exists
if not es.indices.exists(index=ES_INDEX):
    es.indices.create(index=ES_INDEX)

# Function to fetch weather data for multiple cities
def fetch_weather_data():
    for city in CITIES:
        try:
            api_url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={city}&aqi=no"
            response = requests.get(api_url)
            data = response.json()
            
            weather_doc = {
                "city": city,
                "temperature": data["current"]["temp_c"],
                "humidity": data["current"]["humidity"],
                "pressure": data["current"]["pressure_mb"],
                "weather": data["current"]["condition"]["text"],
                "wind_speed": data["current"]["wind_kph"],
                "timestamp": datetime.datetime.utcnow()
            }
            
            # Index data into Elasticsearch
            es.index(index=ES_INDEX, document=weather_doc)
            print(f"Weather data ingested successfully for {city}:", weather_doc)
        except Exception as e:
            print(f"Error fetching or indexing data for {city}:", str(e))

# Schedule data fetching every 10 minutes
schedule.every(10).minutes.do(fetch_weather_data)

# Continuous loop to run scheduler
if __name__ == "__main__":
    fetch_weather_data()  # Initial fetch
    while True:
        schedule.run_pending()
        time.sleep(1)