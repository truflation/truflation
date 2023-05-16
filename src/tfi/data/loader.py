from tfi.data.task import Task

class Loader(Task):
    def __init__(self, reader, writer):
        super().__init__(reader, writer)
    def run(self, source, key):
        d = self.reader.read_all(source)
        self.writer.write_all(d, key=key)
