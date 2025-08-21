import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os
import time

load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
OPENAQ_API_KEY = os.getenv('OPENAQ_API_KEY')

CITIES = ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte']

def fetch_openweather(city):
    print(f"Buscando dados do OpenWeather para {city}")
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fetch OpenWeather para {city}: {e}")
        return None

def fetch_openaq_locations():
    print("Buscando todos os locais do OpenAQ para o Brasil")
    url = "https://api.openaq.org/v3/locations?countries_id=45"
    headers = {
        'X-API-Key': OPENAQ_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fetch OpenAQ locations: {e}")
        return None

def fetch_openaq_sensors(location_id):
    print(f"Buscando sensores para o local {location_id}")
    url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
    headers = {
        'X-API-Key': OPENAQ_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fetch sensores do OpenAQ para o local {location_id}: {e}")
        return None

def extract_data():
    data = {'openweather': {}, 'openaq': {}}
    
    openaq_locations = fetch_openaq_locations()
    
    all_sensor_data = []
    if openaq_locations and 'results' in openaq_locations:
        for location in openaq_locations['results']:
            if location.get('locality') in CITIES:
                time.sleep(1) # to avoid hitting rate limits
                sensor_data = fetch_openaq_sensors(location['id'])
                if sensor_data and 'results' in sensor_data:
                    for sensor in sensor_data['results']:
                        # Add location info to each sensor reading
                        sensor['locality'] = location.get('locality')
                        sensor['location_name'] = location.get('name')
                        all_sensor_data.append(sensor)

    for city in CITIES:
        time.sleep(1)
        data['openweather'][city] = fetch_openweather(city)
    
    data['openaq'] = {city: [s for s in all_sensor_data if s.get('locality') == city] for city in CITIES}

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(f'data/raw_openweather_{timestamp}.json', 'w') as f:
        json.dump(data['openweather'], f)
    with open(f'data/raw_openaq_{timestamp}.json', 'w') as f:
        json.dump(data['openaq'], f)

    print("Dados extraídos com sucesso.")
    return data

if __name__ == "__main__":
    extract_data()