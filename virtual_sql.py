#!/usr/bin/python
import psycopg2
# import cx_Oracle


def virtual_sql(input_sql):
    '''primary interface for virtual sql engine

    keyword_args - takes sql to be executed on virtual engine

    returns -
    return None
'''


def configure_virtual_sql(input_sql_type, target_datastore_type,
                          target_datastore_url, target_datastore_name):
    ''' sets the configuration for the virtual sql interface

        keyword_args:
            input_datastore_type - type of input SQL - Oracle, etc.
            target_datastore_type - type of target datastore - S3, etc.
            target_datastore_url - URL of target datastore, if applicable
                for a database, this should be the database URL
                for S3, this should be 'None'
            target_datastore_name - name of target datastore
                for a database, this should be the database name
                for S3, this should be the bucket name

        returns:
            True if successful
            False if not

'''

    # write configuration to virtuql_sql_configuration.txt
    try:
        with open('virtual_sql_configuration.txt', mode='w') as config_file:
            config_file.write('input_sql_type=' +
                              input_sql_type + '\n')
            config_file.write('target_datastore_type=' +
                              target_datastore_type + '\n')
            config_file.write('target_datastore_url=' +
                              target_datastore_url + '\n')
            config_file.write('target_datastore_name=' +
                              target_datastore_name + '\n')

    except TypeError as te:
        print(te)
        return False

    except PermissionError as pe:
        print(pe)
        return False

    return True


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
        # conn_string = target_datastore_username + '/' + \
        #    target_datastore_password + "@" + target_datastore_url + "/" + \
        #    target_datastore_dbname

        print('Oracle not yet supported')
#        conn = cx_Oracle.connect(conn_string)
#        print(conn.version)
#        conn.close

        return None


if __name__ == '__main__':

    print(configure_virtual_sql('Oracle', 'S3', 'None', 'virtual-data-layer'))
