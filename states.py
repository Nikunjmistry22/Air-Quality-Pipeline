import requests
import time
import json
from constants import API_KEY

country = 'India'
states = [
    "Tripura","Uttar Pradesh"
    "Andaman and Nicobar Islands", "Andhra Pradesh", "Assam", "Bihar", "Chandigarh",
    "Chhattisgarh", "Dadra and Nagar Haveli", "Daman and Diu", "Delhi", "Goa",
    "Gujarat", "Haryana", "Himachal Pradesh", "Jammu and Kashmir", "Jharkhand",
    "Karnataka", "Kerala", "Lakshadweep", "Madhya Pradesh", "Maharashtra",
    "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Puducherry",
    "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal"
]


def get_cities_for_state(state):
    url = f"http://api.airvisual.com/v2/cities?state={state}&country={country}&key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


all_cities = {}


for i, state in enumerate(states):
    try:
        print(f"Fetching cities for state: {state}")
        cities_data = get_cities_for_state(state)
        if cities_data and cities_data['status'] == 'success':
            all_cities[state] = cities_data['data']
            print(f"Success: Data fetched for state: {state}")
        else:
            print(f"Failed to fetch data for state: {state}")
            failed_states.append(state)
    except Exception as e:
        print(f"An error occurred for state: {state}. Error: {e}")
        failed_states.append(state)
    time.sleep(3)
    # Pause after every 5 requests to respect the rate limit
    if (i + 1) % 5 == 0:
        print("Pausing for 60 seconds to respect the rate limit...")
        time.sleep(60)


# Save the data to a JSON file
with open('all_cities_data.json', 'w') as json_file:
    json.dump(all_cities, json_file, indent=4)


print("All cities data fetched and saved successfully.")
