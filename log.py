import logging

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
handler = logging.FileHandler('crawler.log', 'w', 'utf-8')
root_logger.addHandler(handler)

def log_with(func):
    def wrapper(self, *argv, **kwargv):
        result = func(self, *argv, **kwargv)
        logging.info("%s , result: %s", func.__name__, result)
        return result
    return wrapper