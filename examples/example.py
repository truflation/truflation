#!/usr/bin/env python3
# pip install. & &./ examples / example.py

from tfi.data.pipeline import Pipeline
from first import my_pipeline_details


# todo -- add AP scheduler functionality and settings in my details
def main():
    my_pipeline = Pipeline(my_pipeline_details.get_details())
    my_pipeline.ingest()


if __name__ == '__main__':
    main()
