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
import pipeline_run_direct

async def load_path(file_path_list: List[str] | str,
                    debug: bool, dry_run: bool,
                    config: dict | None = None):
    return pipeline_run_direct.load_path(
        file_path_list, debug, dry_run, config
    )

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
