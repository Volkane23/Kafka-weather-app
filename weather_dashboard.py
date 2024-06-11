from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import pandas as pd
import threading

app = Flask(__name__)

# Initialize an empty dictionary to store weather data for each city
weather_data = {}

# Kafka Consumer setup
consumer = KafkaConsumer('weather',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))


def consume_weather_data():
    global weather_data
    for message in consumer:
        data = message.value
        city = data['city']
        weather_data[city] = {
            'weather_data': data['weather_data'],
            'forecast_data': data['forecast_data']
        }
        print(f"Received weather data for {city}")


# Start the Kafka consumer in a separate thread
threading.Thread(target=consume_weather_data, daemon=True).start()

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/weather')
def weather_api():
    print("hello from flask")
    print(weather_data)
    return jsonify(weather_data)
    
if __name__ == '__main__':
    app.run(debug=True)
