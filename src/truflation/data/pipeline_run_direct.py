#!/usr/bin/env python3
# pip install. && ./examples/example.py

"""
Usage:
  pipeline_coupler.py <details_path> [--debug]

Arguments:
  details_path     the relative path to the pipeline details module
"""

import importlib
import logging
from truflation.data.pipeline import Pipeline
from docopt import docopt


def load_path(file_path: str, debug: bool):
    """
Dynamically import and run module, pipeline_details
"""
    if (debug):
        print('debugging')
        logging.basicConfig(level=logging.DEBUG)
    module_name = 'my_pipeline_details'
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'get_details'):
        pipeline_details = module.get_details()
        my_pipeline = Pipeline(pipeline_details)
        my_pipeline.ingest()
    else:
        raise Exception("get_details not found in supplied module,")


if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)
    load_path(args['<details_path>'], args['--debug'])
