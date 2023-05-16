import os
import logging
import shutil
import subprocess
from tfi.data.task import Task
logging.basicConfig(level=logging.DEBUG)

class Validator(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)

    def run(self, name):
        feather_file = f'{name}.feather'
        tdda_file = f'{name}.tdda'
        df = self.reader.read_all(key=name).get()
        df.to_feather(feather_file)
        if not os.path.isfile(tdda_file):
            self.create_constraints(name)

    def create_constraints(self, name):
        tdda_file = f'{name}.tdda'
        feather_file = f'{name}.feather'

        
        df = self.reader.read_all(key=name).get()
        logging.debug(df)
        df.to_feather(feather_file)
        cmd = f'tdda discover {feather_file} {tdda_file}'
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)

    def load_constraints(self, path, name):
        assert os.path.isfile(path)
        shutil.copyfile(f'{path}.tdda', f'{name}.tdda')

    def verify_constraints(self, name):
        tdda_file = f'{name}.tdda'
        feather_file = f'{name}.feather'
        results_file = f'{name}.results'

        df = self.reader.read_all(key=name).get()
        df.to_feather(feather_file)
        cmd = f'tdda detect {feather_file} {tdda_file} {results_file}'
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
        passed = self.check_if_passed_constraints(res.stdout.decode('utf-8'))
        print(f'Ingestor verify test has {"passed" if passed else "failed"}')
        assert passed is True

    @staticmethod
    def check_if_passed_constraints(results: str) -> bool:
        results_list = results.split('\n')
        failing_str = next(x for x in results_list if "Constraints failing: " in x)
        num_failed = int(failing_str.split(": ")[1])
        return num_failed == 0
