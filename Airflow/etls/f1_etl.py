import requests
import pandas as pd
import os
import time
import json



start_year = 2025
years_to_fetch = 3
all_jsons = []


for year in range(years_to_fetch):

    URL = "https://api.openf1.org/v1/sessions"
    INPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/input')
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/output')

    params = {
        "year" : start_year,
    }

    response = requests.get(URL, params=params)


    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)

        os.makedirs(INPUT_DIR, exist_ok=True)
        output_file = os.path.join(INPUT_DIR, f'f1_session_data_{start_year}.json')


        df.to_json(output_file)
        print(f"✅ File saved: {output_file}")

    else:
        print(f"Error: {response.status_code} - {response.text}")

    
    start_year -= 1
    time.sleep(5)


for file in os.listdir(INPUT_DIR):

    if file.endswith('.json'):

        with open(os.path.join(INPUT_DIR, file), 'r') as f:
            data = json.load(f)
            all_jsons.append(data)

combined_file = os.path.join(OUTPUT_DIR, f"f1_session_data_merged_{start_year - years_to_fetch}-{start_year}.json")

with open(combined_file, 'w', encoding='utf-8') as f:
    json.dump(all_jsons, f, indent=2)

print(f"✅ Combined file saved: {combined_file}")