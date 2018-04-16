#!/usr/bin/python
import psycopg2
from virtual_S3_module import execute_sqltree_on_s3
from virtual_postgres_module import convert_SQL_to_postgres, \
                                    execute_sql_on_postgres
from sql_to_tree import sql_to_tree
import get_credentials
# import cx_Oracle


def virtual_sql(input_sql):
    '''primary interface for virtual sql engine

    prereq: configuration is set via configure_virtual_sql command
            credential file is present if target is a database
            AWS account is configured on this program's host if target is S3

    keyword_args - takes sql to be executed on target database

    returns:
        a tuple containing:
            selection_headers - a tuple of the names of the selected fields
            ordered_data - a dataset expressed as a list of tuples
                        each tuple is a row, and the first row is a header

        if there is an error, tuples returned are empty

'''

    # default result is a set of empty tuples
    result = tuple([tuple(), tuple()])

    # read in configuration items
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
                if v.find('credential_file_name=') != -1:
                    configuration['credential_file_name'] = \
                        v.replace('credential_file_name=', '')

    except FileNotFoundError as fe:
        print('Configuration file not found!')
        return result

    # parse SQL into tree
    try:
        sql_tree = sql_to_tree(input_sql)
    except AttributeError as ae:
        print('Unsupported attribute in query')
        return result

    # execute sql tree on target datastore
    if configuration['target_datastore_type'] == 'PostgreSQL':

        # postgres block
        # step 1: get credentials
        # step 2: connect to database
        # step 3: convert SQL to postgres compliance
        # step 4: execute SQL on database and return results

        postgres_credentials = \
            get_credentials.get_credentials(
                                configuration['credential_file_name'])

        postgres_cursor = \
            connect_to_database('PostgreSQL',
                                configuration['target_datastore_url'],
                                configuration['target_datastore_name'],
                                postgres_credentials['username'],
                                postgres_credentials['password'])

        if postgres_cursor is not None:
            postgres_sql = convert_SQL_to_postgres(
                            input_sql,
                            configuration['input_sql_type'])

            result = execute_sql_on_postgres(postgres_sql, postgres_cursor)
        else:
            None

    elif configuration['target_datastore_type'] == 'S3':

        # S3 block
        # step 1: get credentials
        # step 2: connect to database
        # step 3: convert SQL to postgres compliance
        # step 4: execute SQL on database and return results

        try:
            result = execute_sqltree_on_s3(
                        configuration['target_datastore_name'],
                        sql_tree)
        except:
            return result

    else:
        print('Datastore type ' + configuration['target_datastore_type']
              + 'not supported!')

    return result


def configure_virtual_sql(input_sql_type, target_datastore_type,
                          target_datastore_url, target_datastore_name,
                          credential_file_name):
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
            credential_file_name - name of the file containing the credentials
                for the target database.  if S3, this should be 'None'

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
            config_file.write('credential_file_name=' +
                              credential_file_name + '\n')

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

    if target_datastore_type == 'PostgreSQL':
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

    elif target_datastore_type == 'Oracle':
        # conn_string = target_datastore_username + '/' + \
        #    target_datastore_password + "@" + target_datastore_url + "/" + \
        #    target_datastore_dbname

        print('Oracle not yet supported')
#        conn = cx_Oracle.connect(conn_string)
#        print(conn.version)
#        conn.close

        return None

    else:
        print(target_datastore_type + ' not supported yet!')
        return None


if __name__ == '__main__':

    # unit tests

    configure_virtual_sql('Oracle', 'S3', 'None', 'virtual-data-layer', 'None')
    #configure_virtual_sql('Oracle',
    #                      'PostgreSQL',
    #                      'cecs694project-pgsql.coraxglca5kl.us-east-2.rds.amazonaws.com',
    #                      'postgres',
    #                      'postgres_connection.txt')
    result = virtual_sql('select n_name from tcph.nation')
    print(result)
