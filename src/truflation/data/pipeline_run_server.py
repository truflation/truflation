#!/usr/bin/python3

"""
Usage:
  pipeline_run_server.py <details_path> ... [--debug] [--dry_run] [--port=<n>]

Arguments:
  details_path     the relative path to the pipeline details module
"""

import importlib
import logging
from typing import List
from docopt import docopt
from sanic import Sanic
from sanic.response import text, json

from truflation.data.pipeline import Pipeline
from truflation.data.connector import cache_

async def load_path(file_path_list: List[str] | str,
                    debug: bool, dry_run: bool,
                    config: dict | None = None):
    """
    Dynamically import and run module, pipeline_details
    """
    return_value = []

    if config is None:
        config = {}
    cache_.clear()
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

    return return_value

app = Sanic("PipelineServerApp")
args = docopt(__doc__)

@app.get("/hello-world")
async def hello_world(_):
    return text("Hello, world.")

@app.get("/<output>")
async def test(request, output):
    query_params = { key: value[0] for key, value in request.args.items() }
    filelist = [ item for item in args['<details_path>'] if '=' not in item ]
    config = { item.split('=')[0]: item.split('=')[1] \
               for item in args['<details_path>'] if '=' in item }
    await load_path(
        filelist,
        args['--debug'], args['--dry_run'], {**config, **query_params})
    print(cache_.cache_data.keys())
    return json(cache_.get(output))

if __name__ == '__main__':
    port_string = args.get('--port')
    app.run(host='0.0.0.0', port=8000 if port_string is None else int(port_string), debug=True)
