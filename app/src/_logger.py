import logging

class InfoErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno in (logging.INFO, logging.ERROR)

def get_logger(name=__name__):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    logger.propagate = False #Prevent log messaged from being propagated to the root logger

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%m/%d/%Y %I:%M:%S %p')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.addFilter(InfoErrorFilter())
    
    return logger
