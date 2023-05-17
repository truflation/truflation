#!/usr/bin/env python3

'''
#  Temp Dev Notes

# My concerns -- is this smart enough to have n data sources that split into y expors via z calculators
# -- How readily will this scale and what will that solution look like when we have multiple of these reading and exporting, especially if there are needed updates
# -- what if I wanted to add preprocess and post process -- after deploying 80 ingestors

# Cameron wants exports to be one time series of numbers: date / value / created_at
# -- one ingestor should create one table (updateable) -- maybe initial setup is, like creating infrastructure tables, can bypass this rule
# --

# Discussion points
#   Reader, Writer not passed in, using JSON w/ csv/API/excel
#   Considering multiple ins of different types and multiple outs (cameron wants 1)
#   Scaling to many pipelines --
#      -- what would that look like?
#      -- would we still use Docker or would we create a library?
#      --> 3 docker images: modularized pipeline, in-parallel monolith, everything (private)
#   Moving to standard pipeline
#   How to make it versatile so it could load all data across all 'pipelines' first, or do all 'pipelines' fully

# Task --- import CPIH using this system
# Task --- David, do the generalized pipeline using Joseph's updates and feeding in data generally
# Task --- Joseph, consider multiple inputs (of different types) and outputs (to different dbs  and outputting different rows/structures)
# Task --- Jospeh, consider a better way of scaling -- compared to our current scaling solution.
'''



# import sys
# from tfi.data.validator import Validator
# from tfi.data.task import Task
# from tfi.data.loader import Loader
# from tfi.data.data import DataPandas, DataFormat
from tfi.data.pipeline import Pipeline
from first import my_pipeline_details


def main():
    print('====================== example.py ==========================')
    my_pipeline = Pipeline(my_pipeline_details)
    my_pipeline.ingest()


if __name__ == '__main__':
    main()
