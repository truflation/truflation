#!/usr/bin/env python3
# pip install. && ./examples/example.py

"""
Usage:
  pipeline_coupler.py <details_path>

Arguments:
  details_path     the relative path to the pipeline details module
"""

import time
from apscheduler.schedulers.background import BackgroundScheduler
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from docopt import docopt
import importlib


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


def main(pipeline_details: PipeLineDetails):
    """
    Main driver function to set up the pipeline and start the scheduler.

    Retrieves the pipeline details, sets up the scheduler and then runs an infinite loop to keep the script running.
    The scheduler will call the ingest function based on the cron schedule specified in the pipeline details.

    Returns
    -------
    None
    """
    # # Get details for pipeline
    # pipeline_details = get_details()

    # Instantiate scheduler
    scheduler = BackgroundScheduler()

    # Add job based off of cron_schedule in pipeline_details
    scheduler.add_job(ingest,  'cron', **pipeline_details.cron_schedule, args=[pipeline_details])
    scheduler.start()

    # Runs an infinite loop
    while True:
        time.sleep(1)


def load_path(file_path: str):
    # Dynamically import and run module, pipeline_details
    module_name = 'my_pipeline_details'
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'get_details'):
        pipeline_details = module.get_details()
        main(pipeline_details)
    else:
        raise Exception("get_details not found in supplied module,")


if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)
    file_path = args['<details_path>']  # convert path to module name
    load_path(file_path)
