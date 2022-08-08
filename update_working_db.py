from sql_connectors import connect_single


def get_files_to_move(logger, folder):
    query = f''' 
    SELECT file_name 
        FROM heidelberg.working_files
    WHERE up_to_date = False
    AND folder_name = '{folder}'
    '''
    files = [f[0] for f in connect_single(logger, query, get=True)]

    return files


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
    WHERE running_count < 5000
    '''
    folders = [f[0] for f in connect_single(logger, query, get=True)]

    return folders


def update_file(logger, folder, file):
    query = f"""
    UPDATE heidelberg.working_files
        SET up_to_date = True
    WHERE folder_name = '{folder}' 
    AND file_name = '{file}'
    """
    connect_single(logger, query)
