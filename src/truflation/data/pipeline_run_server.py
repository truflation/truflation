#!/usr/bin/python3

"""
Usage:
  pipeline_run_server.py <details_path> ... [--debug] [--dry_run] [--port=<n>]

Arguments:
  details_path     the relative path to the pipeline details module
"""

"""
Issues in pipeline_run_server.py:

The pipeline_run_server.py script isn't currently taking advantage of asynchronous features (async/await).
Although the Sanic web framework it uses supports asynchronous capabilities, the script itself isn't making use of them.
To make the most of asynchronous operations, consider using await for tasks that might cause delays, like reading from disk or network operations.

"""

import importlib
import logging
from typing import List
from docopt import docopt
from fastapi import FastAPI, Request
import uvicorn

from truflation.data.pipeline import Pipeline
from truflation.data.connector import cache_
import pipeline_run_direct

async def load_path(file_path_list: List[str] | str,
                    debug: bool, dry_run: bool,
                    config: dict | None = None):
    if config is None:
        config = {}
    return pipeline_run_direct.load_path(
        file_path_list, debug, dry_run, config | {'clear_cache': False}
    )

app = FastAPI()
args = docopt(__doc__)

@app.get("/hello-world")
async def hello_world():
    return "Hello, world."

@app.get("/{output}")
async def test(output: str, request: Request):
    query_params = { key: value[0] for key, value in request.query_params.items() }
    filelist = [ item for item in args['<details_path>'] if '=' not in item ]
    config = { item.split('=')[0]: item.split('=')[1] \
               for item in args['<details_path>'] if '=' in item }
    cache_.clear()
    await load_path(
        filelist,
        args['--debug'], args['--dry_run'], {**config, **query_params})
    print(cache_.cache_data.keys())
    return cache_.get(output)

if __name__ == '__main__':
    port_string = args.get('--port')
    uvicorn.run(
        app, host='0.0.0.0',
        port=8000 if port_string is None else int(port_string)
    )
