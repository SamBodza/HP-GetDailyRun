
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


