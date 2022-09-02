from sql_connectors import connect_single, connect_single_2

sdb_count_query = """
    SELECT COUNT(id)
	    FROM librarian.raw_file
    WHERE file_name LIKE '%.sdb'
"""

dicom_count_query = """
    SELECT COUNT(id)
        FROM librarian.gcs_file
    WHERE bucket = 'dicom'
"""

folder_count_query = """
    SELECT COUNT(DISTINCT folder_name)
        FROM heidelberg.working_files
"""

processed_folder_count_query = """
    SELECT COUNT(DISTINCT folder_name)
        FROM heidelberg.working_files
    WHERE up_to_date
"""

missing_pdb_query = """
    with PDBS as (
        SELECT DISTINCT folder_name
            FROM heidelberg.working_files
        WHERE file_name LIKE '%.pdb'
    )
    
    SELECT COUNT(DISTINCT folder_name)
        FROM heidelberg.working_files
    WHERE folder_name NOT IN (
		SELECT *
			FROM PDBS
	)
"""


def get_pipeline_data(logger):
    sdb_count = connect_single_2(logger, sdb_count_query, get=True)[0]
    dcm_count = connect_single_2(logger, dicom_count_query, get=True)[0]

    return sdb_count, dcm_count


def get_librarian_data(logger):
    folder_count = connect_single(logger, folder_count_query, get=True)[0]
    p_folder_count = connect_single(logger, processed_folder_count_query, get=True)[0]
    pdb = connect_single(logger, missing_pdb_query, get=True)[0]

    return folder_count, p_folder_count, pdb


def get_infograph_data(logger):
    sdb_count, dcm_count = get_pipeline_data(logger)
    folder_count, p_folder_count, pdb = get_librarian_data(logger)

    print(sdb_count, dcm_count, folder_count, p_folder_count, pdb)

    return sdb_count, dcm_count, folder_count, p_folder_count, pdb


def update_infographic(logger):
    sdb_count, dcm_count, folder_count, p_folder_count, pdb = get_infograph_data(logger)
    insert_into_db_query = f"""
        INSERT INTO heidelberg.working_directory_metadata(
    	    date_ran, number_of_folders, percentage_folders_processed, folders_without_pdbs, sbd_count, dicom_count)
    	VALUES (CURRENT_DATE, {folder_count}, {100 * p_folder_count / folder_count}, {pdb}, {sdb_count}, {dcm_count});

    """
