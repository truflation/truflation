import logging
from truflation.data.task import Task
from truflation.data.connector import Connector
import pandas as pd

logger = logging.getLogger(__name__)


class ExportDetails(Task):
    """
    A class used to represent export details for a data task.

    The class is derived from the Task class and it inherits
    the `reader` and `writer` attributes from it. The class also
    provides functionality for reading and writing data.

    Attributes
    ----------
    name : str
        The name of the export task
    connector : Connector | str
        The connector used for data operations
    key : str
        The key used for reading and writing data
    replace: (default = False)
        replace the table

    Methods
    -------
    read():
        Reads data using the assigned key and returns the result
    write(data):
        Writes the given data using the assigned key
    """
    def __init__(self, name: str, connector: Connector | str, key: str,
                 *args,
                 replace: bool = False,
                 reconcile = None,
                 create_table = None,
                 **kwargs):
        super().__init__(connector, connector)
        self.name = name
        self.key = key
        self.args = args
        self.kwargs = kwargs
        self.replace = replace
        self.reconcile = reconcile
        self.create_table = create_table

    def __repr__(self):
        return "ExportDetails()"

    def __str__(self):
        return f"ExportDetails({self.name},{self.key})"

    def read(self):
        if self.replace:
            return None
        logging.debug(f'key={self.key}')
        try:
            return self.reader.read_all(
                self.key,
                *self.args,
                **self.kwargs
            )
        except FileNotFoundError as e:
            return None

    def write(self, data: pd.DataFrame, **kwargs):
        kwargs['key'] = self.key
        kwargs['if_exists'] = 'replace' if self.replace else 'append'
        if data is not None:
            return self.writer.write_all(data, **kwargs)
        return None


