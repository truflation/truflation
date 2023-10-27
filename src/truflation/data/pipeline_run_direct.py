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
from typing import List, Optional
from truflation.data.pipeline import Pipeline
from truflation.data.general_loader import GeneralLoader
from docopt import docopt

def load_path(file_path_list: List[str] | str,
                    debug: bool, dry_run: bool,
                    config: Optional[dict] = None):
    """
    Dynamically import and run module, pipeline_details
    """
    return_value = []

    if config is None:
        config = {}

    # convert strings to lists
    if isinstance(file_path_list, str):
        file_path_list = file_path_list.split(" ")

    for file_path in file_path_list:
        if debug:
            print('debugging')
            logging.basicConfig(level=logging.DEBUG)
        module_name = 'my_pipeline_details'
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise Exception(f"{file_path} does not exist as a module.")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if hasattr(module, 'get_details_list'):
            return_value.extend([
                Pipeline(detail).ingest(dry_run)
                for detail in module.get_details_list(**config)
            ])
        elif hasattr(module, 'get_details'):
            pipeline_details = module.get_details(**config)
            my_pipeline = Pipeline(pipeline_details)
            return_value.append(my_pipeline.ingest(dry_run))
        else:
            raise Exception("get_details not found in supplied module,")
        if config.get('clear_cache', True):
            loader = GeneralLoader()
            loader.clear()

    return return_value

if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)
    filelist = [ item for item in args['<details_path>'] if '=' not in item ]
    config = { item.split('=')[0]: item.split('=')[1] \
               for item in args['<details_path>'] if '=' in item }

    load_path(filelist,
              args['--debug'], args['--dry_run'], config)

