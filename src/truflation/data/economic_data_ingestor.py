import pandas as pd
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.pipeline import Pipeline


class EconomicDataIngestor:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_data(self):
        try:
            df = pd.read_csv(self.file_path, parse_dates=["Date"])
            return df
        except FileNotFoundError:
            raise Exception(f"The file {self.file_path} was not found.")
        except pd.errors.EmptyDataError:
            raise Exception("No data found in the file.")
        except Exception as e:
            raise Exception(f"An error occurred while reading the file: {str(e)}")

    def process_data(self, df):
        df_cleaned = df.dropna()
        # Additional data processing steps can be implemented here
        return df_cleaned


# Function to create PipelineDetails and integrate the ingestor with the pipeline
def integrate_with_pipeline(
    ingestor, pipeline_name, exports_config, cron_schedule=None
):
    # Define the source details - though in this case, the ingestor handles the source directly
    source_details = SourceDetails(
        name="EconomicDataCSV", source_type="CSV", source=ingestor.file_path
    )

    # Define the pipeline details with the source, exports, and any desired schedule
    pipeline_details = PipeLineDetails(
        name=pipeline_name,
        sources=[source_details],
        exports=exports_config,
        cron_schedule=cron_schedule if cron_schedule else {"hour": "1"},
        pre_ingestion_function=ingestor.read_data,
        post_ingestion_function=lambda: print(
            "Post ingestion steps can be defined here."
        ),
        transformer=ingestor.process_data,
    )

    # Create the Pipeline instance with the details and execute
    pipeline = Pipeline(pipeline_details)
    pipeline.ingest()


#  usage
if __name__ == "__main__":
    file_path = "cci.csv"
    ingestor = EconomicDataIngestor(file_path)
    exports_config = [
        ExportDetails(
            name="DatabaseExport", connector="DBConnector", key="economic_data_table"
        )
    ]

    # Integrate and execute the pipeline
    integrate_with_pipeline(ingestor, "EconomicDataIngestionPipeline", exports_config)
