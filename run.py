from asyncio.log import logger
from distutils.command.config import config
import imp


import logging
import logging.config

import yaml



def main():
    """
    entrypoint of etl
    """

    #parsing yaml file
    config_path = "C:/Users/huang/Documents/xetra_project/xetra_yh/configs/xetra_report1_config.yml"
    config = yaml.safe_load(open(config_path))
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test")


if __name__ == "__main__":
    main()