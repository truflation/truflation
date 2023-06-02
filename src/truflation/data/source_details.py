class SourceDetails:
    def __init__(self, name, source_type, source, connector=None, parser=lambda x: x):
        """
        param:
           name: str:
        """
        self.name = name
        # options: override, csv,
        self.source_type = source_type
        self.source = source
        self.connector = connector # instance of overriden class
        self.parser = parser

