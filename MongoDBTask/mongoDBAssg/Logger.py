import logging


def log(file, log_type):
    if log_type == "INFO":
        level = logging.INFO
    elif log_type == "ERROR":
        level = logging.ERROR
    logging.basicConfig(filename=file, level=level, format='%(asctime)s %(levelname)s %(message)s')
    return logging
