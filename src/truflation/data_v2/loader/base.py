from abc import ABC, abstractmethod
import pandas as pd

class BaseLoader(ABC):
    @abstractmethod
    def load_data(self, df: pd.DataFrame, file_path: str) -> bool:
        """Abstract method to be implemented by subclasses for data loading."""
        pass