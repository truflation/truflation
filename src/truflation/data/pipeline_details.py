from typing import Callable, Dict
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
import pandas


class PipeLineDetails:
    """
    The PipeLineDetails class holds the details for a specific pipeline configuration.
    These details include the name of the pipeline, source details, export details,
    a cron schedule, pre- and post-ingestion functions, and a transformer function.

    This class is intended to provide all the details needed for setting up and
    executing a data pipeline.

    Parameters
    ----------
    name: str
        The name of the pipeline.

    sources: list[SourceDetails]
        A list of SourceDetails objects. Each SourceDetails object contains the
        configuration for a specific data source used in the pipeline.

    exports: list[ExportDetails]
        A list of ExportDetails objects. Each ExportDetails object contains the
        configuration for a specific data export used in the pipeline.

    cron_schedule: dict (default = {'hour': '1'})
        A dictionary representing the cron schedule for running the pipeline.
        By default, the pipeline is scheduled to run at 1 am.

    pre_ingestion_function: Callable (default = None)
        A function to be run before the ingestion process. This is optional
        and by default, no function is run.

    post_ingestion_function: Callable (default = None)
        A function to be run after the ingestion process. This is optional
        and by default, no function is run.

    transformer: Callable[[Dict], Dict] (default = lambda x: x)
        A function that takes a dictionary as input and returns a
        dictionary as output. This is used to perform any necessary
        transformations on the data after it has been ingested and before
        it is exported. By default, this is an identity function that
        returns the input as is.

    Attributes
    ----------
    name: str
        Name of the pipeline.

    sources: list[SourceDetails]
        List of data source configurations for the pipeline.

    exports: list[ExportDetails]
        List of data export configurations for the pipeline.

    cron_schedule: dict
        Cron schedule for the pipeline execution.

    pre_ingestion_function: Callable
        Pre-ingestion function for the pipeline.

    post_ingestion_function: Callable
        Post-ingestion function for the pipeline.

    transformer: Callable[[Dict], Dict]
        Data transformation function for the pipeline.
    """

    def return_none():
        return None

    def return_same(x):
        return x

    def __init__(self,
                 name,
                 sources: list[SourceDetails],
                 exports: list[ExportDetails],
                 cron_schedule: dict = None,
                 pre_ingestion_function: Callable = return_none,
                 post_ingestion_function: Callable = return_none,
                 transformer: Callable[[Dict], Dict] = return_same
                 ):
        self.name = name
        self.sources = sources
        self.exports = exports
        self.cron_schedule = cron_schedule if cron_schedule else {'hour': '1'}  # defaults to 1 am
        self.pre_ingestion_function = pre_ingestion_function
        self.post_ingestion_function = post_ingestion_function
        self.transformer = transformer

    def __str__(self):
        return f"PipeLineDetails({self.name}, cron_schedule: {self.cron_schedule})"

    def __repr__(self):
        return str(self)  # Return the same string representation for both str() and repr()
