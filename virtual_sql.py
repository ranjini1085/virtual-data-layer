#!/usr/bin/python
import psycopg2
from virtual_S3_module import execute_sqltree_on_s3
import virtual_postgres_module
import sql_to_tree
import get_credentials
# import cx_Oracle


def virtual_sql(input_sql):
    '''primary interface for virtual sql engine

    keyword_args - takes sql to be executed on target database

    returns -
        if any errors, returns dataset

'''
    configuration = {}

    try:
        with open('virtual_sql_configuration.txt', mode='r') as config_file:
            config_file = config_file.read().splitlines()

            for i, v in enumerate(config_file):
                if v.find('input_sql_type=') != -1:
                    configuration['input_sql_type'] = \
                        v.replace('input_sql_type=', '')
                if v.find('target_datastore_type=') != -1:
                    configuration['target_datastore_type'] = \
                        v.replace('target_datastore_type=', '')
                if v.find('target_datastore_url=') != -1:
                    configuration['target_datastore_url'] = \
                        v.replace('target_datastore_url=', '')
                if v.find('target_datastore_name=') != -1:
                    configuration['target_datastore_name'] = \
                        v.replace('target_datastore_name=', '')

    except FileNotFoundError as fe:
        print('Configuration file not found!')
        return None

    sql_tree = sql_to_tree.sql_to_tree(input_sql)

    if configuration['target_datastore_type'] == 'PostgreSQL':

        postgres_credentials = \
            get_credentials.get_credentials('postgres_connection.txt')

    elif configuration['target_datastore_type'] == 'S3':
        result = execute_sqltree_on_s3(configuration['target_datastore_name'],
                                       sql_tree)

    else:
        print('Datastore type ' + configuration['target_datastore_type']
              + 'not supported!')

    return result


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
    '''connects to a target database.  only supports postgres at the moment
        not to be used with non-database stores like S3

    keyword_args:
        target_datastore_type
        target_datastore_url
        target_datastore_dbname
        target_datastore_username
        target_datastore_password

    returns:
        a cursor containing the database connection, or None if
            connection cannot be made

'''

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

    configure_virtual_sql('Oracle', 'S3', 'None', 'virtual-data-layer')
    result = virtual_sql('select c_custkey from tcph.customer')
    print(result)
