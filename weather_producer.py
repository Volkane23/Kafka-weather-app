import requests
import json
import time
from kafka import KafkaProducer

# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# API details
API_KEY = 'Your_api_key'
CITIES = ["Casablanca", "Rabat", "Marrakech", "Fez", "Tangier"]

def get_geo_coordinates(city, api_key):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    if data:
        return data[0]['lat'], data[0]['lon']
    else:
        raise Exception(f"Could not get geo coordinates for city: {city}")

coordinates = {}

for city in CITIES:
    lat, lon = get_geo_coordinates(city, API_KEY)
    coordinates[city] = {'lat': lat, 'lon': lon}


def get_weather_data(lat, lon, api_key):
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    weather_response = requests.get(weather_url)
    weather_data = weather_response.json()
    
    forecast_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    forecast_response = requests.get(forecast_url)
    forecast_data = forecast_response.json()
    
    return {
        'weather_data': weather_data,
        'forecast_data': forecast_data
    }

try:
    while True:
        for city, coords in coordinates.items():
            data = get_weather_data(coords['lat'], coords['lon'], API_KEY)
            producer.send('weather', {'city': city, 'weather_data': data['weather_data'], 'forecast_data': data['forecast_data']})
            print(f"Sent weather data and forecast for {city}")
        time.sleep(60)  # Fetch data every hour
except KeyboardInterrupt:
    producer.close()
    print("Kafka producer closed")
