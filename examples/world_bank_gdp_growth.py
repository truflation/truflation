
import pandas as pd
from truflation.data.metadata import Metadata
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.connector import get_database_handle
from dotenv import load_dotenv

load_dotenv()

PIPELINE_NAME = "World Bank Annual GDP Growth"
SOURCE_KEY = 'org.worldbank.gdp.annual'


def pre_ingestion_function():
    """
    Function to perform operations before the data ingestion phase.
    This function is called during the pipeline execution before data loading starts.

    Currently, this function only prints out a simple message.

    Returns
    -------
    None
    """
    print(f'I do this before ingestion')
    
    
def post_ingestion_function():
    """
    Function to perform operations after the data ingestion phase.
    This function is called during the pipeline execution after all data loading is finished.

    Currently, this function only prints out a simple message.

    Returns
    -------
    None
    """
    print(f'I do this after ingestion')
    

def parse_worldbank_annual_gdp_growth(json_arr) -> pd.DataFrame:
    """
    Function to parse World Bank API data for annual GDP growth.

    Args:
        json_arr (list): World Bank API response in JSON format.

    Returns:
        pd.DataFrame: Parsed DataFrame from JSON.
    """
    
    if len(json_arr) < 2 or not isinstance(json_arr[1], list) or len(json_arr[1]) < 1:
        raise Exception("GDP growth data not found")
    
    return_value = {
        'date': [],
        'value': [],
    }
    
    json_values = json_arr[1]
    
    for obj in json_values:
        return_value['date'].append(obj['date'])
        return_value['value'].append(obj['value'])
    
    data_frame = pd.DataFrame(return_value)
    data_frame['date'] = pd.to_datetime(data_frame['date'])
    data_frame.set_index('date', inplace=True)

    return data_frame



def get_details() -> PipeLineDetails:
    """
    Function to get the pipeline details.

    Returns:
        PipeLineDetails: Pipeline details object.
    """
    db_handle = get_database_handle()
    metadata = Metadata(db_handle)
    data_list = metadata.read_by_key(SOURCE_KEY)
    sources = [
        SourceDetails(
            name,
            'http',
            f'https://api.worldbank.org/v2/country/{country_iso}/indicator/NY.GDP.MKTP.KD.ZG?format=json',
            parser=parse_worldbank_annual_gdp_growth
        )
        for name, country_iso in data_list.items()
    ]
    exports = [
        ExportDetails(
            name,
            db_handle,
            name
        )
        for name in data_list
    ]

    return  PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=lambda x: x
    )

if __name__ == "__main__":
    get_details()
