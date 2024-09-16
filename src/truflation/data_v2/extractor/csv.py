import logging
import pandas as pd
from truflation.data_v2.extractor.base import BaseExtractor

class CSVExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()

    def extract_data(self, file_path) -> None | pd.DataFrame:
        """
        Extract data from a CSV file
        
        Args:
        - file_path: str - File path for the input CSV file
        
        Returns:
        pd.DataFrame | None - Extracted data as a DataFrame, or None if an error occurs.
        """
        try:
            data = pd.read_csv(file_path)
            logging.info(f'Data successfully extracted from CSV: {file_path}')
            return data
        except Exception as e:
            logging.error(f'An error occurred while extracting data from CSV: {str(e)}')
            return None
