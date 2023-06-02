#!/usr/bin/env python3
# pip install. && ./examples/example.py

import time
from apscheduler.schedulers.background import BackgroundScheduler
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from my_pipeline_details import get_details


def ingest(pipeline_details: PipeLineDetails):
    """
       Instantiates a Pipeline object and initiates the ingest process.

       Parameters
       ----------
       pipeline_details : PipeLineDetails
           An instance of PipeLineDetails that holds the pipeline's specific details.

       Returns
       -------
       None
       """
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()


def main():
    """
    Main driver function to set up the pipeline and start the scheduler.

    Retrieves the pipeline details, sets up the scheduler and then runs an infinite loop to keep the script running.
    The scheduler will call the ingest function based on the cron schedule specified in the pipeline details.

    Returns
    -------
    None
    """
    # Get details for pipeline
    pipeline_details = get_details()

    # Instantiate scheduler
    scheduler = BackgroundScheduler()

    # Add job based off of cron_schedule in pipeline_details
    scheduler.add_job(ingest,  'cron', **pipeline_details.cron_schedule, args=[pipeline_details])
    scheduler.start()

    # Runs an infinite loop
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
