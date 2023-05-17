class PipeLineDetails:
    def __init__(self):
        self.var = 25
        self.pre_function = None
        self.post_function = None
        self.sources = None
        self.transformer = lambda x: x
        self.export = None

bob = PipeLineDetails()
print(bob.var)
