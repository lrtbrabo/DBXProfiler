import logging

class InfoErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno in (logging.INFO, logging.ERROR)

class LoggerSingleton:
    _instance = None

    def __new__(cls, name=__name__):
        if cls._instance is None:
            cls._instance = super(LoggerSingleton, cls).__new__(cls)
            cls._instance.logger = logging.getLogger(name)
            cls._instance.logger.setLevel(logging.INFO)
            cls._instance.logger.propagate = False  # Prevent log messages from being propagated to the root logger

            if not cls._instance.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%m/%d/%Y %I:%M:%S %p')
                handler.setFormatter(formatter)
                cls._instance.logger.addHandler(handler)
                cls._instance.logger.addFilter(InfoErrorFilter())

        return cls._instance

    def get_logger(self):
        return self.logger

def get_logger(name=__name__):
    return LoggerSingleton(name).get_logger()