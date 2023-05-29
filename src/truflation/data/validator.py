import os
import logging
import shutil
from tdda.constraints import discover_df, verify_df
from truflation.data.task import Task
from truflation.data.connector import connector_factory

logging.basicConfig(level=logging.DEBUG)

class Validator(Task):
    def __init__(self, reader, writer, **kwargs):
        super().__init__(reader, writer)
        self.constraints = connector_factory(
            kwargs.get(
                'constraints',
                'json:'
            )
        )

    def run(self, name):
        tdda_file = f'{name}.tdda'
        if not os.path.isfile(tdda_file):
            self.create_constraints(name)
        self.verify_constraints(name)

    def create_constraints(self, name):
        df = self.reader.read_all(key=name)
        constraints = discover_df(df)
        self.constraints.write_all(
            constraints.to_dict(),
            key = f'{name}.tdda'
        )

    @staticmethod
    def load_constraints(path, name):
        assert os.path.isfile(path)
        shutil.copyfile(f'{path}.tdda', f'{name}.tdda')

    def verify_constraints(self, name):
        tdda_file = f'{name}.tdda'

        df = self.reader.read_all(key=name)
        tdda = self.constraints.read_all(
            f'{name}.tdda'
        )
        v = verify_df(df, tdda)
        logging.debug('Constraints passing: %d', v.passes)
        logging.debug('Constraints failures: %d', v.failures)
        assert v.failures == 0
