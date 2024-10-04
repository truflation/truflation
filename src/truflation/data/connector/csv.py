
import os
import pandas as pd

from pathlib import Path
from typing import Optional
from .base import Connector


class ConnectorCsv(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.path_root = kwargs.get('path_root', os.getcwd())
        Path(self.path_root).mkdir(parents=True, exist_ok=True)

    def read_all(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        Read data from a CSV file.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Optional[pd.DataFrame]: The data read from the CSV file as a pandas DataFrame, or None if the file is not accessible.
        """

        if not len(args):
            raise Exception("need to specify source")
        source = args[0]
        if not source:
            raise Exception("sourc can not be falsey")

        if 'http:' in source or 'https:' in source[:6]:
            # Read the CSV data from the URL
            self.logging_manager.log_info('Reading CSV data...')
            
            try:
                df = pd.read_csv(source, **kwargs)
                self.logging_manager.log_info(f'CSV data successfully read from {source}')
                return df
            except Exception as e:
                self.logging_manager.log_error(f'Error reading CSV data from {source}: {e} ')
                return None

        filename = os.path.join(self.path_root, args[0])
        self.logging_manager.log_info(f'Reading CSV data from file: {filename}')
        self.logging_manager.log_debug(f'args[0]: {args[0]}')
        self.logging_manager.log_debug(f'filename: {filename}')
        # print(f'args[0]: {args[0]}')
        # print(f'filename: {filename}')
        if os.access(filename, os.R_OK):
            try:
                df = pd.read_csv(filename, dtype_backend='pyarrow', **kwargs)
                self.logging_manager.log_info('CSV data successfully read from file.')
                return df
            except Exception as e:
                self.logging_manager.log_error(f'Error reading CSV data from file: {e}')
                return None
        else:
            self.logging_manager.log_warning(f'File {filename} is not accessible.')
            return None

    def write_all(self, data, *args, **kwargs) -> None:
        """
        Write data to a CSV file.

        Args:
            data: The data to be written.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None
        """
        self.logging_manager.log_info('Saving CSV data...')
        
        filename = kwargs.get('key', None)
        if_exists = kwargs.get('if_exists', 'none')
        
        if filename is None and len(args) > 0:
            filename = args[0]
            
        filename = os.path.join(self.path_root, filename)
        self.logging_manager.log_debug(f'File path: {filename}')

        self.write_manifest(filename)
        
        if not os.path.exists(filename):
            self.logging_manager.log_info(f'File {filename} does not exist. Creating a new file.')
            return data.to_csv(
                filename
            )
            
        if if_exists == 'append':
            self.logging_manager.log_info(f'Appending data to file {filename}.')
            return data.to_csv(
                filename, mode='a', header=False
            )
            
        if if_exists == 'replace':
            self.logging_manager.log_info(f'Replacing file {filename} with new data.')
            return data.to_csv(
                filename
            )
        
        self.logging_manager.log_error("Invalid value for 'if_exists' parameter.")
        raise ValueError("Invalid value for 'if_exists' parameter.")
