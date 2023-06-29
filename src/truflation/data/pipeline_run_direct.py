#!/usr/bin/env python3
# pip install. && ./examples/example.py

"""
Usage:
  pipeline_coupler.py <details_path> ... [--debug] [--dry_run]

Arguments:
  details_path     the relative path to the pipeline details module
"""

import importlib
import logging
from truflation.data.pipeline import Pipeline
from docopt import docopt


def load_path(file_path_list, debug: bool, dry_run):
    """
    Dynamically import and run module, pipeline_details
    """
    return_value = []
    for file_path in file_path_list:
        if debug:
            print('debugging')
            logging.basicConfig(level=logging.DEBUG)
        module_name = 'my_pipeline_details'
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if hasattr(module, 'get_details_list'):
            return_value.extend([
                Pipeline(detail).ingest(dry_run)
                for detail in module.get_details_list()
            ])
        elif hasattr(module, 'get_details'):
            pipeline_details = module.get_details()
            my_pipeline = Pipeline(pipeline_details)
            return_value.append(my_pipeline.ingest(dry_run))
        else:
            raise Exception("get_details not found in supplied module,")
    return return_value

if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)

    load_path(args['<details_path>'], args['--debug'], args['--dry_run'])
