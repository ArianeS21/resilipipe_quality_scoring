import logging


def create_logger(logger_name: str = None, output_file: str = None) -> logging.Logger:
    logger = logging.getLogger(logger_name or "Spark logger")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(output_file or "resilipipe.log")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    return logger
