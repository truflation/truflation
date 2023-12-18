from abc import ABC, abstractmethod
import pandas as pd

class BaseExtractor(ABC):
    @abstractmethod
    def extract_data(self, file_path: str) -> None | pd.DataFrame:
        """Abstract method to be implemented by subclasses for data extraction."""
        pass