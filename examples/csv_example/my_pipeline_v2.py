import logging
from truflation.data_v2.pipeline import Pipeline
from truflation.data_v2.types import (
    PipelineDetails,
    Source,
    Destination,
    SourceType,
    DestinationType,
)


def pre_ingestion_function():
    # Add implementation for pre-ingestion function if needed
    pass


def post_ingestion_function():
    # Add implementation for post-ingestion function if needed
    pass


def data_converter(input_data):
    # Add implementation for data transformation
    output_data = input_data
    return output_data


def main():
    # Creating instances of Source and Destination
    source = Source(type=SourceType.CSV, file_path="/source/example.csv")
    destination = Destination(
        type=DestinationType.CSV, file_path="/destination/example.csv"
    )
    # Creating an instance of PipelineDetails
    pipeline_details = PipelineDetails(
        name="myFirstPipeline",
        pre_ingestion_function=pre_ingestion_function,
        post_ingestion_function=post_ingestion_function,
        source=source,
        destination=destination,
        transformer=data_converter,
    )

    my_pipeline = Pipeline(pipeline_details)
    
    # Running the data pipeline
    logging.info("Running the data pipeline...")
    result = my_pipeline.run_pipeline()
    logging.info(f"Data pipeline result: {result}")

    # Scheduling the data pipeline
    cron_schedule = {
        "second": "0",  # At the start of the minute
        "minute": "0",  # At the start of the hour
        "hour": "*",     # Every hour
        "day": "*",      # Every day
        "month": "*",    # Every month
        "year": "*",     # Every year
    }
    logging.info("Scheduling the data pipeline...")
    schedule_result = my_pipeline.schedule_pipeline(cron_schedule)
    logging.info(f"Data pipeline scheduling result: {schedule_result}")


if __name__ == "__main__":
    main()