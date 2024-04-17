import os

import pandas as pd
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
import logging

logger = logging.getLogger()


class HappyIngestor:
    @classmethod
    def pre(cls):
        logger.info("I do this before ingestion")

    @classmethod
    def post(cls):
        logger.info("I do this after ingestion")

    @classmethod
    def process(cls, df) -> pd.DataFrame:
        df['Life Ladder'] = df['Life Ladder'].str.replace(',', '.').astype(float)
        yearly_sum = df.groupby(['year'])['Life Ladder'].agg('sum')
        return yearly_sum


def start(
        pipeline_name: str,
        source: str,
        output: str,
) -> PipeLineDetails:
    ingestor = HappyIngestor()
    sources = SourceDetails(
        name=f"{pipeline_name}-source",
        source_type="csv",
        source=source,
        transformer = ingestor.process,

    )
    exports = ExportDetails(
        name=f"{pipeline_name}-export",
        connector="csv:csv",
        key=output,
    )

    return PipeLineDetails(
        name=pipeline_name,
        sources=[sources],
        exports=[exports],
        pre_ingestion_function=ingestor.pre,
        post_ingestion_function=ingestor.post
    )


if __name__ == "__main__":
    pipeline_name = "happiness_pipeline"
    source_path = os.environ.get("HAPPY_PATH")
    if not os.path.isfile(source_path):
        raise FileNotFoundError
    output = "happy_test_output.csv"
    pipeline_details = start(
        pipeline_name=pipeline_name,
        source=source_path,
        output=output
    )
    Pipeline(pipeline_details).ingest()
