"""
Base Connector
"""

import os
from typing import Iterator, Any
from truflation.data.logging_manager import Logger

class Connector:
    """
    Base class for Import
    """

    def __init__(self, *_args, **_kwargs):
        self.logging_manager = Logger()
        pass

    def authenticate(self, token: str):
        pass

    def read_all(
            self,
            *args,
            **kwargs
    ) -> Any:
        """
        Read Source file and parse through parser

        return: DataPandas, the data, of which a dataframe can be accessed via x.df
        """

        data = None
        while True:
            b = self.read_chunk(b)
            if b is None:
                break
            data = b
        return data

    def read_chunk(
            self,
            outputb,
            *args,
            **kwargs
    ) -> Any:
        return None

    def write_all(
            self,
            data,
            *args, **kwargs
    ) -> None:
        for _ in self.write_chunk(
                data
        ):
            pass

    def write_chunk(
            self,
            data,
            *args, **kwargs
    ) -> Iterator[Any]:
        raise NotImplementedError

    def write_manifest(
            self, 
            filename
    ):
        ## if the environment variable is defined, append the filename to the manifest file.
        MANIFEST_FILE = os.environ.get('PIPELINE_FILES_MANIFEST', None)
        if MANIFEST_FILE != None:
            self.logging_manager.log_info(f'Appending to manifest: {MANIFEST_FILE}')
            os.makedirs(os.path.dirname(MANIFEST_FILE), exist_ok=True)
            with open(MANIFEST_FILE, "a+") as myfile:
                myfile.write(filename + "\n")