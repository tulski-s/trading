import argparse
import logging.config
import os
import yaml


def setup_logging(path='./logging.yaml', logger=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', action='store_true')
    args = parser.parse_args()

    if not logger:
        if args.log == False:
            logger = 'simple_info'
        elif args.log == True:
     	   logger = 'simple_debug'

    if os.path.exists(path):
        with open(path, 'rt') as fh:
            config = yaml.safe_load(fh.read())
        logging.config.dictConfig(config)
        log = logging.getLogger(logger)    
    return log