#!/usr/bin/python3
# pip install. && ./examples/example.py
"""
Usage:
  pipeline_coupler.py <details_path> ... [--cron=<cron_schedule>]

Arguments:
  details_path     the relative path to the pipeline details module
  cron_schedule    file with cron_schedule
"""

import asyncio
import sys
import time
import logging
import json
from apscheduler.schedulers.background import BackgroundScheduler
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from docopt import docopt
import importlib
from pytz import utc

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


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
    logging.debug(pipeline_details)
    if hasattr(pipeline_details, '__iter__'):
        [ Pipeline(detail).ingest() for detail in pipeline_details ]
    else:
        Pipeline(pipeline_details).ingest()


def main(pipeline_details: PipeLineDetails, cron_schedule=None):
    """
    Main driver function to set up the pipeline and start the scheduler.

    Retrieves the pipeline details, sets up the scheduler and then runs an infinite loop to keep the script running.
    The scheduler will call the ingest function based on the cron schedule specified in the pipeline details.

    Returns
    -------
    None
    """
    # Instantiate scheduler with UTC timezone
    scheduler = BackgroundScheduler(timezone=utc)

    if cron_schedule is None:
        if hasattr(pipeline_details, '__iter__'):
            cron_schedule = pipeline_details[0].cron_schedule
        else:
            cron_schedule = pipeline_details.cron_schedule

    # Add job based off of cron_schedule in pipeline_details
    job = scheduler.add_job(ingest,  'cron', **cron_schedule, args=[pipeline_details])
    scheduler.start()

    # Print job and pipeline details so we know it is functioning
    print(f'Scheduling ingestor pipeline {pipeline_details}')
    print(f' --> {job}')

    # Runs an infinite loop
    while True:
        time.sleep(1)


async def load_path(file_path_list: str, cron_schedule=None):
    # Dynamically import and run module, pipeline_details
    pipeline_details_list = []
    for file_path in file_path_list:
        module_name = 'my_pipeline_details'
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            raise Exception(f"{file_path} does not exist as a module.")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if hasattr(module, 'get_details_list'):
            pipeline_details_list.extend(module.get_details_list())
        elif hasattr(module, 'get_details'):
            pipeline_details_list.append(module.get_details())
        else:
            raise Exception("get_details not found in supplied module,")
    main(pipeline_details_list, cron_schedule)

if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)
    logging.debug(args)
    file_path = args['<details_path>']  # convert path to module name
    if args.get('--cron') is None:
        asyncio.run(load_path(file_path))
    else:
        with open(args['--cron']) as cronh:
            cron_schedule = json.load(cronh)
            asyncio.run(load_path(file_path, cron_schedule))

