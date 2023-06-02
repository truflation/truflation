#!/usr/bin/env python3
# pip install. && ./examples/example.py

import time
from apscheduler.schedulers.background import BackgroundScheduler
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from my_pipeline_details import get_details


def ingest(pipeline_details: PipeLineDetails):
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()


def main():
    # Get details for pipeline
    pipeline_details = get_details()

    # Instantiate scheduler
    scheduler = BackgroundScheduler()

    print(f'schedule: {pipeline_details.cron_schedule}')
    # Add job based off of cron_schedule in pipeline_details
    scheduler.add_job(ingest,  'cron', **pipeline_details.cron_schedule, args=[pipeline_details])
    scheduler.start()

    # Runs an infinite loop
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
