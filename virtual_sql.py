#!/usr/bin/python
import psycopg2
# import cx_Oracle


def virtual_sql(input_sql):
'''primary interface for virtual sql engine

    keyword_args - takes sql to be executed on virtual engine

    returns - 
    return None


def configure_virtual_sql():
    None


def connect_to_database(target_datastore_type,
                        target_datastore_url, target_datastore_dbname,
                        target_datastore_username, target_datastore_password):
    # connect to target database

    if target_datastore_type == 'postgres':
        conn_string = "host='" + target_datastore_url + "' dbname='" + \
                      target_datastore_dbname + "' user='" + \
                      target_datastore_username + "' password='" + \
                      target_datastore_password + "'"

        # get a connection, if a connect cannot be made
        #                       an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object,
        #               you can use this cursor to perform queries
        cursor = conn.cursor()

        return cursor

    elif target_datastore_type == 'oracle':
        conn_string = target_datastore_username + '/' + \
            target_datastore_password + "@" + target_datastore_url + "/" + \
            target_datastore_dbname
#        conn = cx_Oracle.connect(conn_string)
#        print(conn.version)
#        conn.close

        return None
