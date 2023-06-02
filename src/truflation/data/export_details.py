import logging
from truflation.data.task import Task
from truflation.data.connector import Connector

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    connector : Connector
        The connector used for data operations
    key : str
        The key used for reading and writing data

    Methods
    -------
    read():
        Reads data using the assigned key and returns the result
    write(data):
        Writes the given data using the assigned key
    """
    def __init__(self, name: str, connector: Connector, key: str):
        super().__init__(connector, connector)
        self.name = name
        self.key = key

    def __repr__(self):
        return "ExportDetails()"

    def __str__(self):
        return f"ExportDetails({self.name},{self.key})"

    def read(self):
        logging.debug(f'key={self.key}')
        return self.reader.read_all(self.key)

    def write(self, data, **kwargs):
        kwargs['key'] = self.key
        return self.writer.write_all(data, **kwargs)

