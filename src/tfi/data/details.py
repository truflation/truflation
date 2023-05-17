class PipeLineDetails:
    def __init__(self, name,
                 sources,
                 exporter,
                 pre_ingestion_function=None,
                 post_ingestion_function=None,
                 transformer=lambda x: x
                 ):
        self.name = name
        self.sources = sources
        self.exporter = exporter
        self.pre_ingestion_function = pre_ingestion_function
        self.post_ingestion_function = post_ingestion_function
        self.transformer = transformer
        self.first = 123
