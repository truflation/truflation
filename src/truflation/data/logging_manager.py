import os
import sys
from datetime import datetime, timezone
from loguru import logger

class Logger:
    level = "INFO"
    stream=sys.stderr
    def __init__(self, log_dir_path = None):
        # Define the directory path
        self.log_dir_path = log_dir_path \
            if log_dir_path is not None \
               else os.environ.get('LOG_DIR_PATH', 'log')

        # Create the log directory if it doesn't exist
        os.makedirs(self.log_dir_path, exist_ok=True)

        # Get the current date
        current_date = datetime.now(timezone.utc).replace(tzinfo=None).date()

        # Concatenate the directory path with the log file name
        self.log_file = os.path.join(self.log_dir_path, current_date.strftime('%Y-%m-%d') + '.log')

        # Configure the logger
        logger.remove() # Remove the default handler
        logger.add(self.log_file, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
                   rotation="10 MB", level="DEBUG")
        logger.add(
            self.stream,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            level=self.level
        )

    @classmethod
    def basic_config(cls, stream=None, level=None):
        if stream is not None:
            cls.stream = stream
        if level is not None:
            cls.level = level

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
