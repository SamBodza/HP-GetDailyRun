import os
from datetime import date

from config_parser import get_config
from create_logger import create_logger
from get_files_to_run import move_folders


def main():
    config = get_config()
    logger = create_logger(os.path.join(config.get('LOGGING', 'LOGGING_PATH'), f'{date.today()}.log'),
                           __file__,
                           config.get('LOGGING', 'LOGGING_LEVEL'))
    move_folders(logger)


if __name__ == '__main__':
    main()
