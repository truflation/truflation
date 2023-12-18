import logging
import pandas as pd
from truflation.data_v2.loader.base import BaseLoader

class CSVLoader(BaseLoader):
    def __init__(self) -> None:
        super.__init__()
    
    def load_data(self, df: pd.DataFrame, file_path: str) -> bool:
        """
        Load the provided DataFrame into a CSV file
        
        Args:
        - df: pd.DataFrame - DataFrame to be loaded into a CSV file
        - file_path: str - File path for the output CSV file
        
        Returns:
        bool - True if the data was successfully loaded, False otherwise
        """
        try:
            df.to_csv(file_path, index=False)
            logging.info(f'Data successfully saved to CSV: {file_path}')
            return True
        except Exception as e:
            logging.error(f'An error occurred while saving data to CSV: {str(e)}')
            return False