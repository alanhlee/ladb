import logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# first file logger
logger_info = setup_logger('info_logger', 'c:/temp/iform_deliveries_logs/info.log')

# second file logger
logger_error = setup_logger('error_logger', 'c:/temp/iform_deliveries_logs/error.log')

# def another_method():
#    # using logger defined above also works here
#    logger.info('Inside method')

