""" This file contains PostgreSQL CRUD Operations.
"""



import psycopg2



def conn_check(USER, DB):
""" Checks connection with the PostgreSQL and returns the remote.
    Requires:	USER: username
	  	DB: Database name
"""
    try:
        connection = psycopg2.connect(user = USER,
                                      database = DB)
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")
        return connection

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
        return None



def insert(USER, DB, table, query, values):
""" Inserts data into table.
    Requires:	USER: username
		DB: Database name
		table: table name
		query: tuple of columns name
		values: tuple values of the column
"""

    connection = conn_check(USER, DB)

    if connection is not None:
        cursor = connection.cursor()

        query = str(query).replace("'","")
        postgres_insert_query = " INSERT INTO "+ table +" "+ str(query) +" VALUES "+ str(values)
        try:
            cursor.execute(postgres_insert_query)
            connection.commit()
            count = cursor.rowcount
            print (count, "Record inserted successfully into "+ table +" table")
        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to insert record into table", error)

        finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    else:
        print("Connection with the database could not be established. Operation failed")


def update(USER, DB, table, id, dict):
""" Updates existing data using id of the tables.
    Requires:	USER: username
		DB: Database name
		table: table name
		id: Unique ID
		dict: Dictionay form of column name and its value
"""
    init = 0
    connection = conn_check(USER, DB)
    if connection is not None:
        cursor = connection.cursor()
        postgres_update_query = " UPDATE "+ table
        for item in dict.items():
            if init == 0:
                if isinstance(item[1], str):
                    postgres_update_query = postgres_update_query +" set "+ str(item[0]) +" = '"+ str(item[1]) +"'"
                else:
                    postgres_update_query = postgres_update_query +" set "+ str(item[0]) +" = "+ str(item[1])
                init = 1
            else:
                if isinstance(item[1], str):
                    postgres_update_query = postgres_update_query +" , "+str(item[0])+" = '"+ str(item[1]) +"'"
                else:
                    postgres_update_query = postgres_update_query +" , "+str(item[0])+" = "+str(item[1])
        postgres_update_query = postgres_update_query +" WHERE id = "+ str(id)
        try:
            cursor.execute(postgres_update_query)
            connection.commit()
            count = cursor.rowcount
            print (count, "Record updated successfully")
        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to update record into table", error)

        finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    else:
        print("Connection with the database could not be established. Operation failed")


def delete(USER, DB, table, id):
""" Deletes existing data using id of the tables.
    Requires:	USER: username
		DB: Database name
		table: table name
		id: Unique ID
"""

    connection = conn_check(USER, DB)
    if connection is not None:
        cursor = connection.cursor()
        sql_delete_query = "Delete from "+ table +" where id = "+str(id)
        try:
            cursor.execute(sql_delete_query)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record deleted successfully ")
        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to delete record from table", error)

        finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    else:
        print("Connection with the database could not be established. Operation failed")

def read(USER, DB, table, id):
""" Fetches existing data using id of the tables.
    Requires:	USER: username
		DB: Database name
		table: table name
		id: Unique ID
"""
    connection = conn_check(USER, DB)

    if connection is not None:
        cursor = connection.cursor()
        sql_read_query = "SELECT * FROM "+ table +" where id = "+str(id)
        try:
            cursor.execute(sql_read_query)
            records = cursor.fetchall()

            for row in records:
                print(row)

        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to fetch record from table", error)

        finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    else:
        print("Connection with the database could not be established. Operation failed")
