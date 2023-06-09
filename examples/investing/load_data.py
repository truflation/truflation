#!/usr/bin/env python3
# pip install. && ./examples/example.py

import logging
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
import pipeline_details

logging.basicConfig(level=logging.DEBUG)

def main():
    details = pipeline_details.get_details()
    my_pipeline = Pipeline(details)
    my_pipeline.ingest()



if __name__ == '__main__':
    main()
