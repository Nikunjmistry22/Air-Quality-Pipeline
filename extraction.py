import requests
import pandas as pd
import json
import time
from datetime import datetime
from constants import API_KEY
def extract_data():
    # Load the JSON data
    with open('test_data.json', 'r') as file:
        data = json.load(file)

    
    country = "India"
    base_url = "http://api.airvisual.com/v2/city"

    # Initialize an empty list to store the results
    results = []

    # Iterate over each city in the JSON data
    for state, cities in data.items():
        for i, city_info in enumerate(cities):
            city = city_info['city']
            url = f"{base_url}?city={city}&state={state}&country={country}&key={API_KEY}"
            response = requests.get(url)
            
            if response.status_code == 200:
                response_data = response.json()
                if response_data['status'] == 'success':
                    print(f"Success: Data fetched for city: {city}, state: {state}")
                    city_data = response_data['data']
                    pollution = city_data['current']['pollution']
                    weather = city_data['current']['weather']
                    
                    # Append the data to the results list
                    results.append({
                        "city": city_data['city'],
                        "state": city_data['state'],
                        "country": city_data['country'],
                        "latitude": city_data['location']['coordinates'][1],
                        "longitude": city_data['location']['coordinates'][0],
                        "pollution_ts": pollution['ts'],
                        "aqius": pollution['aqius'],
                        "mainus": pollution['mainus'],
                        "aqicn": pollution['aqicn'],
                        "maincn": pollution['maincn'],
                        "weather_ts": weather['ts'],
                        "temperature": weather['tp'],
                        "pressure": weather['pr'],
                        "humidity": weather['hu'],
                        "wind_speed": weather['ws'],
                        "wind_direction": weather['wd'],
                        "icon": weather['ic'],
                        "date_fetched": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                else:
                    print(f"Failed: Data fetched for city: {city}, state: {state}")
            else:
                print(f"Error: Failed to fetch data for city: {city}, state: {state}, status code: {response.status_code}")
                time.sleep(3)
            # Pause for 3 seconds between requests
            time.sleep(3)
            
            if (i+1)% 5 == 0 and i>0:
                print("Pausing for 60 seconds due to rate limit...")
                time.sleep(60)

    # Convert the results list to a pandas DataFrame
    df = pd.DataFrame(results)
    print(df)
    return df