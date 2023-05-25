from tfi.data.task import Task
import logging

logging.basicConfig(level=logging.DEBUG)

class ExportDetails(Task):
    def __init__(self, name, connector, key):
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

