#!/usr/bin/python3

"""
Usage:
  pipeline_run_server.py <details_path> ... [--debug] [--dry_run] [--port=<n>]

Arguments:
  details_path     the relative path to the pipeline details module
"""

"""
Issues in pipeline_run_server.py:
The pipeline_run_server.py script doesn't currently utilize asynchronous features (async/await).
The Sanic web framework used for the server provides asynchronous capabilities, but the script does not take advantage of them.
For asynchronous operations, consider using await for potentially blocking operations, such as reading from the disk or network.
"""

"""
Describe package integration with asyncio:
To integrate with asyncio, you need to use libraries and functions that support asynchronous execution.
If your package involves I/O-bound operations (e.g., network requests), consider using aiohttp for asynchronous HTTP requests.
Modify functions in your code to use the async/await syntax where appropriate.
If there are functions in the pipeline_run_server.py script that can benefit from asynchronous execution (e.g., reading from disk, network operations), refactor them to be asynchronous.
"""

"""
3. Ideas to make this package work with low-latency systems:
Connection Pooling: Implement connection pooling for database connections or external services to reduce connection overhead.
Caching: Optimize data retrieval by implementing a caching mechanism to store frequently accessed data.
Asynchronous I/O: Use asynchronous libraries for I/O operations to minimize latency in handling multiple requests concurrently.
4. Performance benchmark systems:
Utilize Benchmarking Libraries: Use Python benchmarking libraries like timeit, pytest-benchmark, or asyncio benchmarking libraries.
Define Benchmark Scenarios: Create benchmark scenarios that simulate real-world usage patterns, including both low and high load situations.
Measure and Analyze: Measure the performance of critical sections of your code and analyze the results to identify potential bottlenecks.
Optimize and Retest: After identifying bottlenecks, optimize the corresponding sections and retest to ensure improvements.
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
    if config is None:
        config = {}
    return pipeline_run_direct.load_path(
        file_path_list, debug, dry_run, config | {'clear_cache': False}
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
    cache_.clear()
    await load_path(
        filelist,
        args['--debug'], args['--dry_run'], {**config, **query_params})
    print(cache_.cache_data.keys())
    return json(cache_.get(output))

if __name__ == '__main__':
    port_string = args.get('--port')
    app.run(host='0.0.0.0', port=8000 if port_string is None else int(port_string), debug=True)
