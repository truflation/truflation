import logging
import os
# from telegram_bot.utilities import log_to_bot
#from telegram_bot.general_logger import log_to_bot

# Define your logger function
def my_logger(msg):
    # Put your logging code here
    # log_path = os.getenv('GENERAL_LOGFILE', "temporary_pipeline_logging_location")
    # print(f"My Logger: {msg}")
    my_dic_msg = {"error": msg}
    #log_to_bot(my_dic_msg)
    # todo -- send to Telegram logger

#  todo - think how to incorporate unique/personal logging (not in the main code)
# todo -- default to printing
# todo -- clean Telegram bot code, make a library


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
