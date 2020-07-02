
from crud_opp import conn_check, insert

def duplicate(USER, DB, src, tr):
""" Used to duplicate entries of table src to another esiting table tr.
    Requires:	USER: username
		DB: Database name
		src: source table from where the data is to be extracted
		tr: target table from where the data is to be inserted
"""
    connection = conn_check(USER, DB)
    if connection is not None:
        src_cursor = connection.cursor()
        sql_read_query = "SELECT * FROM "+ str(src)
        try:
            src_cursor.execute(sql_read_query)
            src_records = src_cursor.fetchall()

            src_rows_query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+src+"'"
            src_cursor.execute(src_rows_query)
            src_rows = src_cursor.fetchall()
            rows = []
            for i in range(len(src_rows)):
                rows.append(src_rows[i][0])
            records = str(src_records).replace("[", "")
            records = str(records).replace("]", "")

            insert(USER, DB, tr, tuple(rows), records)

        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to fetch record from table", error)

duplicate("kushagra", "kushagra", "marks", "marks_dup")
