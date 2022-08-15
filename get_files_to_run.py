import os
import re

from config_parser import get_config
from teams_notifs import send_error_msg
from update_working_db import (get_folders_to_sync,
                               get_files_to_move,
                               update_file)


def create_dir(logger, folder):
    try:
        paths = get_config()['PATHS']
        path = os.path.join(paths['dst_dir'], folder)
        os.mkdir(path)
    except Exception as e:
        logger.critical(f'unable to create folder {folder}: {e}')
        send_error_msg('Failed to create Dir, check logs for more details')
        raise


def move_folder(logger, folder):
    paths = get_config()['PATHS']
    files = get_files_to_move(logger, folder)
    if files:
        create_dir(logger, folder)
        for file in files:
            try:
                src = os.path.join(paths['src_dir'], folder, file)
                dst = os.path.join(paths['dst_dir'], folder, file)
                command = f'rsync -av {src} {dst}'
                _ = os.popen(command).read()
                if not re.match("^.+\.pdb$", file):
                    update_file(logger, folder, file)
            except Exception as e:
                logger.error(f'failed to sync {folder}/{file}: {e}')
                send_error_msg('Failed to sync folder, check logs for more details')


def move_folders(logger):
    folders = get_folders_to_sync(logger)
    for folder in folders:
        move_folder(logger, folder)
