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
    latest_only: (default = True)
        when reconciling against existing data, compare incoming rows only
        against the most recent row per identifier instead of all history,
        so a corrected value can displace a stale "latest" row. Ignored if
        a custom `reconcile` callable is supplied.

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
                 latest_only: bool = True,
                 create_table = None,
                 **kwargs):
        super().__init__(connector, connector)
        self.name = name
        self.key = key
        self.args = args
        self.kwargs = kwargs
        self.replace = replace
        self.reconcile = reconcile
        self.latest_only = latest_only
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
        combined_kwargs = {**getattr(self, 'kwargs', {}), **kwargs}
        combined_kwargs['key'] = self.key
        combined_kwargs['if_exists'] = 'replace' if self.replace else 'append'
        if data is not None:
            return self.writer.write_all(data, **combined_kwargs)
        return None


