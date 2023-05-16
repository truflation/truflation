from tfi.data.task import Task

class Validator(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)

    def run(self, key):
        pass
