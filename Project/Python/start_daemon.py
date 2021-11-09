import daemon
from main import run
from src.loggers import logger

with daemon.DaemonContext(files_preserve=[logger.fh.stream.fileno()]):
    run()