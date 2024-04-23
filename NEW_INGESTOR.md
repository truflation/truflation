# EconomicDataIngestor Documentation
## Overview
`EconomicDataIngestor` is a Python class designed to ingest and process economic data from CSV files for use in the Truflation data pipeline. It aims to streamline the extraction of economic indicators, ensuring data cleanliness and consistency before further processing or storage.

## Installation

The `EconomicDataIngestor` module is placed within the Truflation project structure, ideally under `truflation/data/`. Use the following command to copy the ingestor to the appropriate directory within your virtual environment:

`cp ./src/truflation/data/economic_data_ingestor.py ./env/lib/python3.11/site-packages/truflation/data/`

Dependencies

 - Python 3.11
 - pandas

Ensure all dependencies are installed within your environment:

`pip install pandas`

## Usage

### Basic Usage

To use the `EconomicDataIngestor`, first, ensure you have a CSV file with economic data, formatted with at least 'Date' and 'Value' columns. Initialize the ingestor with the path to your CSV file:

```
from truflation.data.economic_data_ingestor import EconomicDataIngestor

file_path = 'path/to/your/cci.csv'
ingestor = EconomicDataIngestor(file_path)
```
 

To read and process the data:

```
df = ingestor.read_data()
processed_df = ingestor.process_data(df)

```

To run the code: 

`python3 src/truflation/data/economic_data_ingestor.py`

### Integration with Truflation Pipeline

After instantiating the EconomicDataIngestor, you can integrate it into the Truflation pipeline system using the provided integration function:


`integrate_with_pipeline(ingestor)`

This function requires the ingestor instance and properly configured PipelineDetails.

## Testing
Unit tests for
 EconomicDataIngestor are provided to ensure its correct functionality. To run the tests, navigate to your project directory and execute:


`pytest tests/test_economic_data_ingestor.py -s`

Ensure the pytest package is installed in your environment.

## Extending
`EconomicDataIngestor` can be extended to handle different formats or sources of economic data. To extend its functionality:

 1. Subclass EconomicDataIngestor and override the read_data and process_data methods as needed.
 2. Ensure to implement error handling and data validation according to your data source's specifics.
