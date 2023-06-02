from truflation.data.task import Task
from truflation.data.connector import Connector


class Loader(Task):
    """
    The Loader class is responsible for reading data from a source and writing
    it to a destination, which could be any type of data store (e.g., file,
    database, etc.). It inherits from the Task class.

    This class mainly serves as an interface to handle the extraction and loading
    parts of an ETL (Extract, Transform, Load) pipeline.

    Parameters
    ----------
    reader: str | Connector
        The reader object or a string representing the reader, which is
        responsible for reading data from a specified source.

    writer: str | Connector
        The writer object or a string representing the writer, which is
        responsible for writing data to a specified destination.

    Attributes
    ----------
    reader: Connector
        The reader object, responsible for reading data.

    writer: Connector
        The writer object, responsible for writing data.

    Methods
    -------
    run(source: str, key: str) -> None
        Reads data from the given source using the reader, writes the data to a
        destination using the writer.
    """
    def __init__(self, reader: str | Connector, writer: str | Connector):
        super().__init__(reader, writer)

    def run(self, source: str, key: str):
        """
        Executes the loader's primary function of reading data from a source and
        writing it to a destination.

        Parameters
        ----------
        source: str
            The source from which to read the data.

        key: str
            The key associated with the data. This could be a filename,
            database key, etc., depending on the specific reader and writer.

        Returns
        -------
        None
        """
        d = self.reader.read_all(source)
        self.writer.write_all(d, key=key)
