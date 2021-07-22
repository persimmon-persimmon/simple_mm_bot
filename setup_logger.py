
import logging
def setup_logger(name,logfile = None):
    logger = logging.getLogger(str(name))
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    if logfile is not None:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(formatter)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
    return logger