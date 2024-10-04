import json
import os
import pandas as pd
from pathlib import Path
from typing import Any
from .base import Connector


class ConnectorJson(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.path_root = kwargs.get('path_root', os.getcwd())
        Path(self.path_root).mkdir(parents=True, exist_ok=True)

    def read_all(
            self, *args, **kwargs
    ) -> Any:
        self.logging_manager.log_info('Loading JSON file...')
        
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
            
        if isinstance(filename, str):
            filepath = os.path.join(self.path_root, filename)
            self.logging_manager.log_info(f'Loading JSON file from: {filepath}')
            try:
                with open(filepath) as fileh:
                    obj = json.load(fileh)
                    self.logging_manager.log_info('JSON file loaded successfully.')
                    return obj
            except FileNotFoundError:
                self.logging_manager.log_error(f'File not found: {filepath}')
            except Exception as e:
                self.logging_manager.log_error(f"Error loading JSON file: {e}")
                return None
        else:
            return json.load(filename)

    def write_all(
            self,
            data,
            *args, **kwargs) -> None:
        self.logging_manager.log_info('Saving data to file...')
        
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
            
        # looks like filename will always be an instance of str?
        if isinstance(filename, str):
            filename = os.path.join(self.path_root, filename)
            self.write_manifest(filename)

            self.logging_manager.log_info(f'Writing data to file: {filename}')
            try:
                with open(filename, 'w') as fileh:
                    fileh.write(json.dumps(data, default=str))
                self.logging_manager.log_info('Data successfully written to file.')
            except Exception as e:
                self.logging_manager.log_error(f'Error writing data to file: {e}')
        else:
            if isinstance(data, str):
                self.logging_manager.log_info('Writing string data to file.')
                print(data, file=filename)
            elif isinstance(data, pd.DataFrame):
                self.logging_manager.log_info('Writing DataFrame to file')
                filename.write(data.to_json(**kwargs))
            else:
                filename.write(json.dumps(data, default=str))
