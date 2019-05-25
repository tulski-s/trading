# buil in
import argparse
import logging
import logging.config
import os

# thrid party
import yaml


def setup_logging(path='./logging.yaml', logger=None, debug=False):
    if isinstance(logger, logging.RootLogger) or isinstance(logger, logging.Logger):
        return logger
    if not logger:
        if debug == False:
            logger = 'simple_info'
        elif debug == True:
     	   logger = 'simple_debug'
    if os.path.exists(path):
        with open(path, 'rt') as fh:
            config = yaml.safe_load(fh.read())
        logging.config.dictConfig(config)
    return logging.getLogger(logger)


def get_parser():
    """
    Returns argparse.parser obj with agruments common for more modules.
    Can be extened arbitrarly in each modul level if more customization needed.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    return parser
