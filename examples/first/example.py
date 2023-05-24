#!/usr/bin/env python3
# pip install. && ./examples/example.py

from tfi.data.pipeline import Pipeline
from tfi.data.pipeline_details import PipeLineDetails
import my_pipeline_details
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import time


# todo -- consider loading in all pipeline_details from a directory
# todo -- add AP scheduler functionality and settings in my details
def ingest(pipeline_details: PipeLineDetails):
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()


def main():
    pipeline_details = my_pipeline_details.get_details()
    # scheduling_details = pipeline_details.scheduling_details

    scheduler = BackgroundScheduler()
    scheduler.add_job(ingest,  'interval', seconds=15, args=[pipeline_details])


    # todo -- pass in scheduling information from pipeline_details
    # In 2022-4-30 12:00:00 Run once job Method
    # scheduler.add_job(ingest, 'date', run_date=datetime.datetime(2022, 4, 30, 12, 0, 0), rgs=['Job 2'])

    # Runs from Monday to Friday at 6:30AM until 2022-06-30 00:00:00
    # scheduler.add_job(ingest, 'cron', day_of_week='mon-fri', hour=6, minute=30, end_date='2022-06-30', args=['job 3'])

    # Runs every 5 minutes at 17 o'clock a day
    # scheduler.add_job(ingest, 'cron', hour=17, minute='*/5', args=['job 2'])

    scheduler.start()

    # Runs an infinite loop
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
