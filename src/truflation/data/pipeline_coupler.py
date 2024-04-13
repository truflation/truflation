#!/usr/bin/python3
# pip install. && ./examples/example.py
"""
Usage:
  pipeline_coupler.py <details_path> ... [--cron=<cron_schedule>]

Arguments:
  details_path     the relative path to the pipeline details module
  cron_schedule    file with cron_schedule
"""

import sys
import time
import logging
import json
import importlib

from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from docopt import docopt
from pytz import utc
from telegram_bot.general_logger import log_to_bot

from truflation.data.pipeline import Pipeline
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def ingest(module_list: list, config: dict):
    """
       Instantiates a Pipeline object and initiates the ingest process.

       Parameters
       ----------
       module_list: List of modules to run

       Returns
       -------
       None
       """
    pipeline_details_list = []
    for module in module_list:
        if hasattr(module, 'get_details_list'):
            pipeline_details_list.extend(module.get_details_list(**config))
        elif hasattr(module, 'get_details'):
            pipeline_details_list.append(module.get_details(**config))
        else:
            raise Exception("get_details not found in supplied module,")

    logging.debug(pipeline_details_list)
    [ Pipeline(detail).ingest() for detail in pipeline_details_list ]

def main(module_list: list, cron_schedule=None):
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
        if hasattr(module_list[0], 'get_details_list'):
            cron_schedule = module_list[0].get_details_list(**config)[0].cron_schedule
        else:
            cron_schedule = module_list[0].get_details(**config).cron_schedule

    if hasattr(module_list[0], 'get_details_list'):
        pipeline_name = module_list[0].get_details_list(**config)[0].name
    else:
        pipeline_name = module_list[0].get_details(**config).name


    # Add job based off of cron_schedule in pipeline_details
    job = scheduler.add_job(ingest,
                            'cron',
                            **cron_schedule, args=[module_list, config])
    scheduler.start()

    # Print job and pipeline details so we know it is functioning
    print('Scheduling ingestor pipelines')
    print(f' --> {job}')

    # log start of pipeline
    log_to_bot(f'{pipeline_name} has been scheduled: {job}')

    # Runs an infinite loop
    while True:
        time.sleep(1)

def load_path(
        file_path_list: List[str], cron_schedule=None,
        config=None
):
    if config is None:
        config = {}

    # Dynamically import and run module, pipeline_details
    module_list = []
    for file_path in file_path_list:
        module_name = 'my_pipeline_details'
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise Exception(f"{file_path} does not exist as a module.")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module_list.append(module)

    main(module_list, cron_schedule)

if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)
    logging.debug(args)
    file_path = [ item for item in args['<details_path>'] if '=' not in item ]
    config = { item.split('=')[0]: item.split('=')[1] \
               for item in args['<details_path>'] if '=' in item }

    if args.get('--cron') is None:
        load_path(file_path, None, config)
    else:
        with open(args['--cron'], encoding='utf-8') as cronh:
            cron_schedule = json.load(cronh)
            load_path(file_path, cron_schedule, config)
