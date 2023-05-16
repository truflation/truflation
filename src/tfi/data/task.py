class Task:
    def __init__(self, reader=None, writer=None, **kwargs):
        self.reader = reader
        self.writer = writer
        pass

    def authenticate(self, token):
        pass

    def run(self, *args, **kwargs):
        raise NotImplementedError
