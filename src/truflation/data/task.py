from truflation.data.connector import connector_factory

class Task:
    def __init__(self, reader=None, writer=None, **kwargs):
        self.reader = connector_factory(reader) \
            if isinstance(reader, str) \
            else reader
        self.writer = connector_factory(writer) \
            if isinstance(writer, str) \
            else writer

    def authenticate(self, token):
        pass

    def run(self, *args, **kwargs):
        raise NotImplementedError
