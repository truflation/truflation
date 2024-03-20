import os
from datetime import datetime
from loguru import logger

class Logger:
    def __init__(self, log_dir_path = '../../../log'):
        # Define the directory path
        self.log_dir_path = log_dir_path
        
        # Create the log directory if it doesn't exist
        os.makedirs(self.log_dir_path, exist_ok=True)

        # Get the current date
        current_date = datetime.utcnow().date()
        
        # Concatenate the directory path with the log file name
        self.log_file = os.path.join(self.log_dir_path, current_date.strftime('%Y-%m-%d') + '.log')
        
        # Configure the logger
        logger.remove() # Remove the default handler
        logger.add(self.log_file, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB", level="DEBUG")
    
    def log_info(self, message):
        """
        Log an informational message.
        
        Parameters:
            message (str): The message to be logged.
        """
        
        logger.info(message)
    
    def log_debug(self, message):
        """
        Log a debug message.
        
        Parameters:
            message (str): The message to be logged.
        """
        
        logger.debug(message)
    
    def log_warning(self, message):
        """
        Log a warning message.
        
        Parameters:
            message (str): The message to be logged.
        """
        
        logger.warning(message)
    
    def log_error(self, message):
        """
        Log an error message.
        
        Parameters:
            message (str): The message to be logged.
        """
        
        logger.error(message)
    
    def log_exception(self, message):
        """
        Log an exception message.
        
        Parameters:
            message (str): The message to be logged.
        """
        
        logger.exception(message)

# if __name__ == '__main__':
#     my_logger = Logger()
    
#     my_logger.log_debug("Hello, World!")