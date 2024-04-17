import pytest
import responses
from truflation.data.pipeline import Pipeline
from ..imf_gb_pipeline import get_details
import logging
from pathlib import Path
from pandas import read_csv
import numpy
from typing import Mapping, Any
import os
import json


def load_response(response_name: str) -> Mapping[str, Any]:
    with open(Path(os.path.dirname(__file__)) / response_name, "r") as response:
        return json.load(response)


@responses.activate
def test_pipeline(caplog):
    resp = responses.get(
        "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.GB.PMP_IX",
        json=load_response("sample_response.json"),
    )
    pipeline_details = get_details()
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()
    csv_export = my_pipeline.exports[0]
    df = read_csv(Path(csv_export.writer.path_root) / csv_export.name)
    assert resp.call_count == 1
    assert len(df) == 5
