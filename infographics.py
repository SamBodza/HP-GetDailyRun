from sql_connectors import connect_single

error_query = """
    with errs as (
        SELECT error_message, COUNT(error_message) as freq
            FROM librarian.parse_metadata
        GROUP BY error_message
    )
    
    SELECT *
        FROM errs
    WHERE freq > 0
        ORDER BY freq DESC
"""

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

pdb_perc_query = """
    with total as (
        SELECT COUNT(DISTINCT(folder_name))
            FROM heidelberg.working_files
        WHERE up_to_date = True),
    
    pdbs as (SELECT (COUNT(DISTINCT(file_name)))
        FROM heidelberg.working_files
    WHERE folder_name IN (
        SELECT DISTINCT(folder_name)
            FROM heidelberg.working_files
        WHERE up_to_date = True)
    AND file_name LIKE '%.pdb')
    
    SELECT (100.0 * (SELECT * FROM pdbs) / (SELECT * FROM total)) as pdb_perc
"""






