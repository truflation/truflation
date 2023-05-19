from tfi.data.connector import connector_factory
from tfi.data.export_details import ExportDetails


class Exporter:
    def __init__(self):
        pass
    # def __init__(self, export_details: ExportDetails):
    #     self.name = export_details.name
    #     self.ip = export_details.ip
    #     self.port = export_details.port
    #     self.username = export_details.username
    #     self.password = export_details.password

    @staticmethod
    def export(export_details: ExportDetails, df):
        print(f'todo -- exporting to {export_details.username}@{export_details.ip}:{export_details.port} -> \n{df}')

    @staticmethod
    def export_dump(export_details: ExportDetails, sql_file_path):
        print(f'todo -- exporting df: {df} to {export_details.username}@{export_details.ip}:{export_details.port}')

