import os
import logging
import shutil
from tdda.constraints import discover_df, verify_df
from truflation.data.task import Task
from truflation.data.connector import connector_factory, Connector
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Validator(Task):
    """
    Validator is a component in the Pipeline used to validate the ingested data
    based on specific rules or conditions. The validation rules are generated
    from the constraints provided by the TDDA (Test-Driven Data Analysis) library.

    This class inherits from the Task class, which encapsulates the functionality
    needed to read, write, and perform tasks on data.

    The class uses a reader and writer object for data reading and writing, and
    a constraints object to handle constraints. These objects are created via
    a connector_factory function based on the connector details provided in the kwargs.

    Methods
    -------
    run(name: str) -> None:
        Executes the constraints creation and verification process for a specific
        data set, identified by the 'name' parameter.

    create_constraints(name: str) -> None:
        Discovers and creates TDDA constraints for a specific data set, identified
        by the 'name' parameter.

    load_constraints(path: str, name: str) -> None:
        Loads constraints from a file at the specified 'path' and copies them to a
        file identified by the 'name' parameter.

    verify_constraints(name: str) -> None:
        Verifies the constraints for a specific data set, identified by the 'name' parameter.

    Parameters
    ----------
    reader: str | Connector
        A string or Connector object used for reading data.
    writer: str | Connector
        A string or Connector object used for writing data.
    kwargs: dict
        A dictionary of keyword arguments. It should include the connector details for the
        'constraints' object.

    Attributes
    ----------
    constraints: object
        An object used for handling constraints.

    Raises
    ------
    AssertionError:
        If the number of constraint failures is not 0 or the constraints file does not exist.
    """

    def __init__(self, reader: str | Connector, writer: str | Connector, **kwargs):
        super().__init__(reader, writer)
        self.constraints = connector_factory(
            kwargs.get(
                'constraints',
                'json:'
            )
        )

    def run(self, name: str) -> None:
        tdda_file = f'{name}.tdda'
        if not os.path.isfile(tdda_file):
            self.create_constraints(name)
        self.verify_constraints(name)

    def create_constraints(self, name: str) -> None:
        df = self.reader.read_all(key=name)
        constraints = discover_df(df)
        self.constraints.write_all(
            constraints.to_dict(),
            key = f'{name}.tdda'
        )

    @staticmethod
    def load_constraints(path, name: str) -> None:
        assert os.path.isfile(path)
        shutil.copyfile(f'{path}.tdda', f'{name}.tdda')

    def verify_constraints(self, name: str)-> None:
        tdda_file = f'{name}.tdda'

        df = self.reader.read_all(key=name)
        tdda = self.constraints.read_all(
            f'{name}.tdda'
        )
        v = verify_df(df, tdda)
        logger.debug('Constraints passing: %d', v.passes)
        logger.debug('Constraints failures: %d', v.failures)
        assert v.failures == 0
