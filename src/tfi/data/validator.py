import os
import logging
import shutil
from tdda.constraints import discover_df, verify_df
from tfi.data.task import Task
logging.basicConfig(level=logging.DEBUG)

class Validator(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)

    def run(self, name):
        tdda_file = f'{name}.tdda'
        if not os.path.isfile(tdda_file):
            self.create_constraints(name)
        self.verify_constraints(name)

    def create_constraints(self, name):
        tdda_file = f'{name}.tdda'
        df = self.reader.read_all(key=name).get()
        constraints = discover_df(df)
        with open(tdda_file, 'w') as f:
            f.write(constraints.to_json())

    @staticmethod
    def load_constraints(path, name):
        assert os.path.isfile(path)
        shutil.copyfile(f'{path}.tdda', f'{name}.tdda')

    def verify_constraints(self, name):
        tdda_file = f'{name}.tdda'

        df = self.reader.read_all(key=name).get()
        v = verify_df(df, tdda_file)
        logging.debug('Constraints passing: %d', v.passes)
        logging.debug('Constraints failures: %d', v.failures)
        assert v.failures == 0
