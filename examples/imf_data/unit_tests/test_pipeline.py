import pytest
from truflation.data.pipeline import Pipeline
from ..imf_gb_pipeline import get_details
import logging
from pathlib import Path
from pandas import read_csv
import numpy
from typing import Mapping, Any
import os
import json
from icecream import ic


def test_pipeline(caplog):
    os.environ['CONNECTOR'] = 'csv:/tmp/truflation'
    pipeline_details = get_details()
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()
    csv_export = my_pipeline.exports[0]
    ic(csv_export)
    df = read_csv(Path(csv_export.writer.path_root) / csv_export.name)
    assert len(df) > 10
