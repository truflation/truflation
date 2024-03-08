import subprocess
import time
import pandas as pd

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

def main():
    script_path = './fetchAvgData.js'  # Path to your JS script
    json_file_path = './FindingHistoricData.json'  # Path to the output JSON file

    while True:
        print("Running JS script...")
        run_js_script(script_path)

        print("Loading JSON file into DataFrame...")
        df = load_json_to_dataframe(json_file_path)

        if df is not None:
            print(df.head())  # Display the first few rows of the DataFrame

        time.sleep(60 * 10)  # Wait for 10 min (60 seconds * 10 minutes) before repeating

if __name__ == "__main__":
    main()
