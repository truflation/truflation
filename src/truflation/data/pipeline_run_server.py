#!/usr/bin/python3
"""
Usage:
  pipeline_run_server.py <details_path> ... [--debug] [--dry_run] [--port=<n>]

Arguments:
  details_path     the relative path to the pipeline details module
"""

from typing import List
from docopt import docopt
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from truflation.data.connector import cache_
from truflation.data import pipeline_run_direct

import os
import uvicorn
import sentry_sdk

load_dotenv()

"""
Issues in pipeline_run_server.py:

The pipeline_run_server.py script isn't currently taking
advantage of asynchronous features (async/await).

Although the Sanic web framework it uses supports asynchronous
capabilities, the script itself isn't making use of them.
To make the most of asynchronous operations, consider using
await for tasks that might cause delays, like reading from disk
or network operations.

"""


async def load_path(file_path_list: List[str] | str,
                    debug: bool, dry_run: bool,
                    config: dict | None = None):
    if config is None:
        config = {}
    return pipeline_run_direct.load_path(
        file_path_list, debug, dry_run, False, config | {'clear_cache': False}
    )

app = FastAPI()
args = {}

@app.get("/hello-world")
async def hello_world():
    return "Hello, world."

@app.get("/{output}")
async def test(output: str, request: Request):
    query_params = dict(request.query_params.items())
    filelist = [ item for item in args['<details_path>'] if '=' not in item ]
    config = { item.split('=')[0]: item.split('=')[1] \
               for item in args['<details_path>'] if '=' in item }
    cache_.clear()
    await load_path(
        filelist,
        args['--debug'], args['--dry_run'], {**config, **query_params})
    return cache_.get(output)

if __name__ == '__main__':
    args = docopt(__doc__)
    port_string = args.get('--port')

    if os.getenv('SENTRY_ENVIRONMENT') == 'production':
        sentry_sdk.init(
            dsn="https://3cd6f73e3e74c407db4c879519297db9@o4507655688880128.ingest.us.sentry.io/4507824420093952",
            traces_sample_rate=0.2,
            profiles_sample_rate=0.2
        )

    uvicorn.run(
        app, host='0.0.0.0',
        port=8000 if port_string is None else int(port_string)
    )
