import os
from typing import Dict

import pandas as pd
from dotenv import load_dotenv

from truflation.data.export_details import ExportDetails
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails

load_dotenv()

# Name
pipeline_name = "IMF GB Inflation"


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


def parse_imf_data(json_obj: Dict) -> Dict:
    """
    Function to parse investing.com json object.
    Args:
        json_obj (Dict): investing.com json object.
    Returns:
        pd.DataFrame: Parsed dataframe from json.
    """
    print(2)
    return json_obj['CompactData']['DataSet']['Series']


sources = [
    SourceDetails(
        "imf_gb_inflation",
        "rest+http",
        "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.GB.PMP_IX",
        parser=parse_imf_data
    )
]

# Schedule to work every month
cron_schedule = {
    "second": "0",
    "minute": "0",
    "hour": "0",
    "day": "1",
    "month": "1",
    "year": "*",
}


def transformer(data_dict: dict):
    """
    Performs a specific transformation operation on the input dataframes.
    The transformation is applied on compact data from

    Parameters
    ----------
    data_dict : dict
        A dictionary where the key is the name of the dataframe, and the value is the dataframe itself.

    Returns
    -------
    dict
        A dictionary containing the transformed dataframes.
    """

    data = data_dict['imf_gb_inflation']

    # Create pandas dataframe from the observations
    data_list = [[obs.get('@TIME_PERIOD'), obs.get('@OBS_VALUE')]
                 for obs in data['Obs']]

    df = pd.DataFrame(data_list, columns=['date', 'value'])

    df = df.set_index(pd.to_datetime(df['date']))['value'].astype('float')

    res_dict = {"imf_gb_inflation": df}
    return res_dict


def get_details():
    """
    Retrieves the details of a pipeline and returns a `PipeLineDetails` object.

    This function loads environment variables, defines sources and exports,
    and creates a `PipeLineDetails` object to represent the pipeline.
    The pipeline includes pre-ingestion and post-ingestion functions,
    a transformer for data transformation, and a cron schedule for pipeline execution.

    Returns
    -------
    PipeLineDetails
        A `PipeLineDetails` object that contains all of the details for the pipeline.
    """
    # Used csv for test
    CONNECTOR = os.getenv('CONNECTOR', "csv:/tmp/truflation")

    if CONNECTOR is None:
        raise Exception("CONNECTOR not found in environment")

    exports = [
        ExportDetails(
            name='imf_gb_inflation',
            connector=CONNECTOR,
            key='imf_gb_inflation'
        )
    ]

    my_pipeline = PipeLineDetails(name=pipeline_name,
                                  sources=sources,
                                  exports=exports,
                                  cron_schedule=cron_schedule,
                                  pre_ingestion_function=pre_ingestion_function,
                                  post_ingestion_function=post_ingestion_function,
                                  transformer=transformer
                                  )
    return my_pipeline


if __name__ == "__main__":
    get_details()
