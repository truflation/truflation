#!/usr/bin/env python3
"""
This pipeline calculates index such as the electric vehicle index
"""

import logging
import time
from pympler import asizeof, muppy, tracker, summary
from pympler import refbrowser
from datetime import datetime, timezone
import pandas as pd
import tracemalloc
import numpy as np
from icecream import ic

import pandas as pd
import random
from datetime import datetime, timedelta
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails

logger = logging.getLogger(__name__)
# Name
PIPELINE_NAME = "Index calc"

def create_large_dataframe(num_rows):
    dates = datetime.now().strftime('%Y-%m-%d')
    values = [random.uniform(0, 1) for _ in range(num_rows)]
    df = pd.DataFrame({'date': dates, 'value': values}).set_index('date')
    return df

def transform(inputs):
    in_table = inputs.get('input',
                         create_large_dataframe(500000))
    values = [random.uniform(0, 1) for _ in range(len(in_table))]
    in_table['value'] = values
    return {'result': in_table}

def get_details(**config):
    sources = [
        SourceDetails(
            'input',
            'csv',
            'test.csv'
        )
    ]

    exports = [
        ExportDetails(
            'result',
            'csv',
            'test.csv',
            replace=True
        )
    ]

    return PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=transform
    )

def output_function(o):
    return str(type(o))

if __name__ == "__main__":
    tracemalloc.start()
    pipeline_details = get_details()
    tr = tracker.SummaryTracker()
    my_pipeline = Pipeline(pipeline_details)
    while True:
        my_pipeline.ingest()
        ic(tracemalloc.get_traced_memory())
        all_objects = muppy.get_objects()
        sum1 = summary.summarize(all_objects)
        summary.print_(sum1)
        my_pipeline.clear()


