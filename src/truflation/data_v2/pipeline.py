import logging
from typing import Dict, Union
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from truflation.data_v2.cache import Cache
from truflation.data_v2.types import PipelineDetails
from truflation.data_v2.factory import ExtractorFactory, LoaderFactory


class Pipeline(Cache):
    """
    A class representing a data pipeline for extracting, transforming, and loading data.
    """

    def __init__(self, pipeline_details: PipelineDetails):
        """
        Initialize the Pipeline object with pipeline details.

        Args:
        - pipeline_details: PipelineDetails - Details of the data pipeline.
        """
        super().__init__()
        self.name = pipeline_details.name
        self.pre_ingestion_function = pipeline_details.pre_ingestion_function
        self.post_ingestion_function = pipeline_details.post_ingestion_function
        self.source = pipeline_details.source
        self.destination = pipeline_details.destination
        self.transformer = pipeline_details.transformer

    def run_pipeline(self) -> Union[None, Dict]:
        """
        Run the data pipeline, performing data extraction, transformation, and loading.

        Returns:
        None | Dict - Result of running the data pipeline.
        """
        try:
            logging.info("Data pipeline started.")
            self.pre_ingestion_function()

            extracted_data = self._extract_data()
            transformed_data = self._transform_data(extracted_data)
            self._load_data(transformed_data)

            self.post_ingestion_function()
            logging.info("Data pipeline completed.")
            return {"status": "success"}
        except Exception as e:
            logging.error(f"Error in data pipeline: {str(e)}")
            return {"status": "failed", "error": str(e)}

    def schedule_pipeline(self, cron_schedule) -> Union[None, Dict]:
        """
        Schedule the data pipeline to run based on the given cron schedule.

        Args:
        - cron_schedule: str - Cron schedule for when to run the pipeline.

        Returns:
        None | Dict - Result of scheduling the data pipeline.
        """
        try:
            scheduler = BackgroundScheduler(timezone=utc)
            scheduler.add_job(self.run_pipeline, "cron", cron_schedule)
            scheduler.start()
            logging.info("Data pipeline scheduled successfully.")
            return {"status": "scheduled"}
        except Exception as e:
            logging.error(f"Error scheduling data pipeline: {str(e)}")
            return {"status": "failed", "error": str(e)}

    def _extract_data(self) -> pd.DataFrame:
        """
        Extract data from the source using the appropriate extractor.
        
        Returns:
        pd.DataFrame - Extracted data in a DataFrame.
        """
        extractor = ExtractorFactory.create_extractor(self.source.type)
        return extractor.extract(self.source.file_path)

    def _load_data(self, df: pd.DataFrame) -> None:
        """
        Load the provided DataFrame into the destination using the appropriate loader.
        
        Args:
        - df: pd.DataFrame - DataFrame to be loaded into the destination.
        """
        loader = LoaderFactory.create_loader(self.destination.type)
        loader.load(df, self.destination.file_path)

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the provided DataFrame using the defined transformation function.
        
        Args:
        - df: pd.DataFrame - DataFrame to be transformed
        
        Returns:
        pd.DataFrame - Transformed DataFrame.
        """
        return self.transformer(df)