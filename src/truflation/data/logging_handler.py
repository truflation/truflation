import logging


# Define your logger function
def my_logger(msg):
    # Put your logging code here
    print(f"My Logger: {msg}")
    # todo -- send to Telegram logger


# Define a handler that calls your custom function
class CustomHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        my_logger(log_entry)


def get_handler():
    # Set up Python logging
    handler = CustomHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    return handler

    # Add the custom handler to the root logger
    # logging.getLogger('').addHandler(handler)
