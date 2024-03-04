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
    with open(csv_file, 'w') as file:
        file.write(csv_data)
    print('CSV file has been created!')

# Specify the file paths
json_file_path = 'historicalData.json'
csv_file_path = 'data.csv'

# Run the function
fetch_data_and_convert_to_csv(json_file_path, csv_file_path)
