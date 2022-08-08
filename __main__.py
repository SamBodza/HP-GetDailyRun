import os
from datetime import date

from config_parser import get_config
from create_logger import create_logger
from get_files_to_run import get_folders_to_sync, move_folders


def main():
    config = get_config()
    logger = create_logger(os.path.join(config.get('LOGGING', 'LOGGING_PATH'), f'{date.today()}.log'),
                           __file__,
                           config.get('LOGGING', 'LOGGING_LEVEL'))
    folders = get_folders_to_sync(logger)
    move_folders(logger, folders)


if __name__ == '__main__':
    main()
