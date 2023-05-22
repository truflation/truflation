class ExportDetails:
    def __init__(self, name, host, port, db, table, username, password):
        self.name = name
        self.host = host
        self.port = port
        self.db = db
        self.table = table
        self.username = username
        self.password = password

    def __repr__(self):
        return "ExportDetails()"

    def __str__(self):
        return f"ExportDetails({self.username}@{self.host}:{self.port}, db:{self.db})"

