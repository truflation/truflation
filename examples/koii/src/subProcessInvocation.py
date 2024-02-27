import subprocess
import time
import pandas as pd
import json

def run_js_script(script_path):
    try:
        subprocess.run(['node', script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the script: {e}")

def load_json_to_dataframe(json_file_path):
    try:
        df = pd.read_json(json_file_path)
        return df
    except Exception as e:
        print(f"An error occurred while loading the JSON file: {e}")
        return None
    
    import json

def fetch_data_and_convert_to_csv(json_file, csv_file):
    # Read JSON data from file
    with open(json_file, 'r') as file:
        json_data = json.load(file)

    # Prepare the CSV header
    csv_header = 'Round'
    cities = ['Denver', 'Boston', 'Brooklyn', 'Chicago', 'Seattle', 'Miami', 'Washington, D.C.', 'Los Angeles']
    categories = ['Economy', 'Compact', 'Intermediate', 'Standard', 'Standard SUV']

    for city in cities:
        for category in categories:
            csv_header += f",{city}-{category}"

    csv_data = f"{csv_header}\n"

    # Process each round
    for round, data in json_data.items():
        # The key for the current data changes for each round
        curr_data_key = str(list(data.get("curr_data", {}).keys())[0])
        curr_data = data.get("curr_data", {}).get(curr_data_key, {})
        csv_row = [round]

        for city in cities:
            city_data = curr_data.get(city, {})
            csv_row.extend([city_data.get(category, "") for category in categories])

        csv_data += ','.join(map(str, csv_row)) + '\n'

    # Write CSV data to a file
        
    print(csv_data)
    with open(csv_file, 'w') as file:
        file.write(csv_data)
    print('CSV file has been created!')

# Specify the file paths

def main():
    script_path = './src/fetchAvgData.js'  # Path to your JS script
    json_file_path = '/output/FindingHistoricData.json'  # Path to the output JSON file
    csv_file_path = '/output/data.csv'

    while True:
        print("Running JS script...")
        run_js_script(script_path)

        print("Loading JSON file into DataFrame...")
        df = load_json_to_dataframe(json_file_path)

        if df is not None:
            print(df.head())  # Display the first few rows of the DataFrame
            # Run the function
            fetch_data_and_convert_to_csv(json_file_path, csv_file_path)

        time.sleep(60 * 10)  # Wait for 10 min (60 seconds * 10 minutes) before repeating

if __name__ == "__main__":
    main()
