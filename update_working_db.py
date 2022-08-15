from sql_connectors import connect_single
from teams_notifs import send_error_msg


def get_files_to_move(logger, folder):
    query = f''' 
    SELECT file_name 
        FROM heidelberg.working_files
    WHERE up_to_date = False
    AND folder_name = '{folder}'
    '''

    try:
        files = [f[0] for f in connect_single(logger, query, get=True)]
        return files
    except Exception as e:
        logger.error(f'failed to retrieve files from db : {e}')
        send_error_msg('Failed to retrieve files from db, check logs for more details')


def get_folders_to_sync(logger):
    query = '''
    with data as (
            SELECT
          folder_name,
          (sum(file_count) over 
          (order by folder_name asc rows between unbounded preceding and current row)) as running_count
        from (
            SELECT folder_name, 
                    COUNT(file_name) as file_count
                FROM heidelberg.working_files
            WHERE up_to_date = False
            AND file_name LIKE '%.sdb'
            GROUP BY folder_name
            ORDER BY folder_name
        ) as q)


    SELECT folder_name 
        FROM data
    WHERE running_count < 15000
    '''

    try:
        folders = [f[0] for f in connect_single(logger, query, get=True)]
        return folders
    except Exception as e:
        logger.critical(f'failed to retrieve folders from db : {e}')
        send_error_msg('Failed to retrieve folders from db, check logs for more details')
        raise


def update_file(logger, folder, file):
    query = f"""
    UPDATE heidelberg.working_files
        SET up_to_date = True
    WHERE folder_name = '{folder}' 
    AND file_name = '{file}'
    """
    try:
        connect_single(logger, query)
    except Exception as e:
        logger.error(f'failed to update folder from db : {e}')
        send_error_msg('Failed to update folder from db, check logs for more details')
