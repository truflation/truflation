from typing import Any
from .base import Connector

class ConnectorDirect(Connector):
    """ used for reading in DataFrames, dictionaries, json files directly as objects. Can not be written to???
    kwargs:
      data_type: str: the type of object that is passed in
    """

    def __init__(self, *args, **kwargs):
        super().__init__()

    def read_all(
            self, *args, **kwargs
    ) -> Any:
        self.logging_manager.log_info('Validating data type...')
        
        data_type = kwargs.get('data_type', None)
        data = kwargs.get('data', None)
        
        try:
            assert data_type is not None, "no data type specified"
            assert data is not None, "no data provided"
            assert isinstance(data, data_type), "data does not match data_type"
            self.logging_manager.log_info('Data type validation successful.')
            return data

        except AssertionError as e:
            self.logging_manager.log_error(f'Data type validation failed: {e}')
            return None

    def write_all(
            self,
            _,
            *_args, **_kwargs) -> None:
        raise NotImplementedError

