import logging
from src.configs.config import LOG_PATH


def print_log(string, error=False):
    print(string)

    if error:
        log.error(string)
    else:
        log.info(string)


logging.basicConfig(filename=LOG_PATH, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
fh = logging.FileHandler(LOG_PATH)
log = logging.getLogger('app')
log.addHandler(fh)