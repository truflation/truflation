#!/usr/bin/env python3
# pip install. && ./examples/example.py

"""
Usage:
  pipeline_coupler.py <details_path> ... [--debug] [--dry_run] [--fail_through]

Arguments:
  details_path     the relative path to the pipeline details module
  --debug          turns on debug output
  --dry_run        do not export data
  --fail_through   re-raise the exception if an exception is encountered in the pipeline script execution
"""

import importlib
from truflation.data.pipeline import Pipeline
from truflation.data.general_loader import GeneralLoader
from truflation.data.logging_manager import Logger
from docopt import docopt

def load_path(file_path_list: list[str] | str,
                    debug: bool, dry_run: bool,
                    fail_through: bool,
                    config: dict | None = None):
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
            Logger.basic_config(level="DEBUG")
        module_name = 'my_pipeline_details'
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise Exception(f"{file_path} does not exist as a module.")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        loader = GeneralLoader()
        if hasattr(module, 'get_details_list'):
            for detail in module.get_details_list(**config):
                my_pipeline = Pipeline(detail)
                return_value.append(my_pipeline.ingest(dry_run, fail_through))
                if config.get('clear_cache', True):
                    loader.clear()
        elif hasattr(module, 'get_details'):
            detail = module.get_details(**config)
            my_pipeline = Pipeline(detail)
            return_value.append(my_pipeline.ingest(dry_run, fail_through))
            if config.get('clear_cache', True):
                loader.clear()
        else:
            raise Exception("get_details not found in supplied module,")
    return return_value

if __name__ == '__main__':
    # Get file_path from argument
    args = docopt(__doc__)
    filelist = [ item for item in args['<details_path>'] if '=' not in item ]
    config = { item.split('=')[0]: item.split('=')[1] \
               for item in args['<details_path>'] if '=' in item }

    load_path(filelist,
              args['--debug'], args['--dry_run'], args['--fail_through'], config)

