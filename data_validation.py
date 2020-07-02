"""Some tools to validate the data in PostgreSQL"""

from crud_opp import conn_check

def drop_na(USER, DB, table):
""" Drops data where any column value is NaN
"""
    connection = conn_check(USER, DB)
    if connection is not None:
        cursor = connection.cursor()
        sql_empty_query = "delete from "+ table +" where not ("+table+" is not null)"
        try:
            cursor.execute(sql_empty_query)
            connection.commit()
            count = cursor.rowcount
            print (count, "Record cleaned from empty values successfully from "+ table +" table")
        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to remove record from table", error)

drop_na("kushagra", "kushagra", "marks_dup")
