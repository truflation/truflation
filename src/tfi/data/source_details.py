class SourceDetails:
    def __init__(self, name, source_type, source, parser=lambda x: x):
        self.name = name
        self.source_type = source_type
        self.source = source
        self.parser = parser
