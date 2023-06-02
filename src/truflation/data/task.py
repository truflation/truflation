from truflation.data.connector import connector_factory, Connector


class Task:
    """
    The Task class is a base class for creating tasks in the Pipeline. It provides
    functionality to initialize a reader and writer for data processing tasks. These
    reader and writer are either Connector objects or string specifications which
    are converted to Connector objects using the connector_factory function.

    This class also defines a general structure for tasks by providing a
    'run' method which must be implemented in any child class, along with an
    'authenticate' method which can be overridden as required.

    Methods
    -------
    authenticate(token: str) -> None:
        Authenticates a task using a provided token. It is a placeholder and should
        be overridden in the child classes as needed.

    run(*args, **kwargs):
        Executes the task. This method must be implemented in any child class.

    Parameters
    ----------
    reader: str | Connector (default = None)
        A string or Connector object used for reading data. If a string is passed, it
        is converted into a Connector object using the connector_factory function.

    writer: str | Connector (default = None)
        A string or Connector object used for writing data. If a string is passed, it
        is converted into a Connector object using the connector_factory function.

    kwargs: dict
        A dictionary of keyword arguments.

    Attributes
    ----------
    reader: Connector
        A Connector object used for reading data.

    writer: Connector
        A Connector object used for writing data.

    Raises
    ------
    NotImplementedError:
        If the run method is not implemented in a child class.
    """

    def __init__(self, reader: str | Connector = None, writer: str | Connector = None, **kwargs):
        self.reader = connector_factory(reader) \
            if isinstance(reader, str) \
            else reader
        self.writer = connector_factory(writer) \
            if isinstance(writer, str) \
            else writer

    def authenticate(self, token: str):
        pass

    def run(self, *args, **kwargs):
        raise NotImplementedError
