#!/usr/bin/python
import boto3
import csv
from botocore.exceptions import EndpointConnectionError


def retrieve_s3_file(bucket, filename, folder=None):
    '''retrieves file from S3

    keyword-args:
        bucket = name of S3 bucket from which to download file
        filename = name of S3 file to be downloaded
        folder = name of folder within S3, if any (optional)

    returns:
        name of downloaded file on local filesystem
'''
#   Create an S3 client
    s3 = boto3.client('s3')

#   set local and S3 filenames
    if folder is not None:
        local_filename = folder+'_'+filename
        s3_filename = folder+'/'+filename
    else:
        local_filename = filename
        s3_filename = filename

#   download file
    try:
        None
        with open(local_filename, "wb") as s3_file:
            s3.download_fileobj(bucket, s3_filename, s3_file)
    except EndpointConnectionError:
        print('cannot connect to S3 bucket')

#   return name of local file
    return local_filename


def file_to_data_structure(local_filename, sql_tree=None):
    '''converts a local file to a data structure, applying filters along the way

    keyword_args:
        local_filename: name of the local CSV that should be converted to a
                        data file_to_data_structure
            assumes that the first column of this file is a header
                        with column column_names
            assumes that this file is comma delimited

        sql_tree (optional): the SQL tree associated with this file,
                        used to apply filters

    returns:
        column positions: a dictionary with a mapping between column names
                        and column_positions
        file_data: a list of rows from the local file, with each row
                        represented as a list
'''

    file_data = []
    column_positions = {}
    data_filter_sql_tree = []
    data_filter_this_file = {}

    if sql_tree is not None and 'filters' in sql_tree:
        data_filter_sql_tree = sql_tree['filters']

    lfile_reader = csv.reader(open(local_filename, newline=''),
                              delimiter=',', quotechar='"')

    column_names = lfile_reader.__next__()
    for i, v in enumerate(column_names):
        column_positions[v] = i

    # check to see if filter column is in file and map filters to keys
    for i, filter in enumerate(data_filter_sql_tree):
        filter_identifier = filter['identifier']
        filter_value = filter['value']
        filter_operator = filter['operator']

        if filter_identifier in column_positions:
            filter_position = column_positions[filter_identifier]
            if filter_position not in data_filter_this_file:
                data_filter_this_file[filter_position] = []
            filter_values = {}
            filter_values['operator'] = filter_operator
            filter_values['value'] = filter_value
            data_filter_this_file[filter_position].append(filter_values)

    # read file into data structure, filtering along the way
    # for line in lfile:
    for line_data in lfile_reader:
        # if any filters,
        # filter file on filter column while reading into dataset
        if len(data_filter_this_file) == 0:
            file_data.append(line_data)
        elif len(data_filter_this_file) > 0:
            for filter_position, filter_list in \
                    data_filter_this_file.items():
                for i, filter in enumerate(filter_list):
                    if filter['operator'] == '=' \
                            and line_data[filter_position] == \
                            filter['value'].replace("'", ''):
                        file_data.append(line_data)

                    if filter['operator'] == '!=' \
                            and line_data[filter_position] != \
                            filter['value'].replace("'", ''):
                        file_data.append(line_data)

                    if filter['operator'] == '>' \
                            and float(line_data[filter_position]) > \
                            float(filter['value'].replace("'", '')):
                        file_data.append(line_data)

                    if filter['operator'] == '>=' \
                            and float(line_data[filter_position]) >= \
                            float(filter['value'].replace("'", '')):
                        file_data.append(line_data)

                    if filter['operator'] == '<' \
                            and float(line_data[filter_position]) < \
                            float(filter['value'].replace("'", '')):
                        file_data.append(line_data)

                    if filter['operator'] == '<=' \
                            and float(line_data[filter_position]) <= \
                            float(filter['value'].replace("'", '')):
                        file_data.append(line_data)

    return column_positions, file_data


def map_select_columns_to_data(sql_tree, table_name, column_positions):
    '''

'''
    selected_column = {}
    select_identifiers = sql_tree['select']
    select_identifiers.extend(sql_tree['select aggregate'])

    for i, v in enumerate(select_identifiers):
        if v['column_name'] is not None:
            column_name = v['column_name']
        if column_name in column_positions.keys():
            selected_column[column_name] = \
                (table_name, column_positions[column_name])
    return selected_column


def transpose_columns_to_rows(selected_data_in_columns):
    '''

'''
    selected_data_in_rows = []
    selected_data_headers = list(selected_data_in_columns.keys())

    for row, value in \
            enumerate(selected_data_in_columns[selected_data_headers[0]]):
        row_data = []
        for k in selected_data_in_columns.keys():
            row_data.append(selected_data_in_columns[k][row])
        selected_data_in_rows.append(row_data)

    return selected_data_headers, selected_data_in_rows


def exectute_sqltree_on_s3(bucket, sql_tree):
    query_data = {}
    query_data_column_positions = {}
    selected_columns = []
    selected_data = {}

    # download files in query and map to data structures
    for table_definition in sql_tree['table_definitions']:
        alias = ''
        s3_folder = table_definition['schema']
        s3_filename = table_definition['name']
        if table_definition['alias'] is not None:
            alias = table_definition['alias']
        else:
            alias = table_definition['name']

        query_data_column_positions[alias], query_data[alias] = \
            file_to_data_structure(retrieve_s3_file(bucket,
                                                    s3_filename, s3_folder),
                                   sql_tree)

    # map selected columns to tables
    for k in query_data_column_positions:
        selected_columns.append(
                map_select_columns_to_data(sql_tree, k,
                                           query_data_column_positions[k]))

    # apply joins

    # select columns from specified dataset
    for i, column in enumerate(selected_columns):
        for k in column.keys():
            selected_data[k] = []
            select_table = column[k][0]
            selected_column_position = column[k][1]

            for row in query_data[select_table]:
                selected_data[k].append(row[selected_column_position])

    # apply aggregates and having

    # apply order by
    selected_headers, selected_rows = transpose_columns_to_rows(selected_data)
    ordered_data = [selected_headers]

    # add support for extracing the columns to be ordered
    # add support for ordering asc or desc
    # figure out how to sort on multiple keys
    if 'ordering' in sql_tree.keys():
    #    for i, order_item sql_tree['ordering'].items():
        ordered_data.extend(sorted(selected_rows, key=lambda i: float(i[0])))
    else:
        ordered_data.extend(selected_rows)

    return ordered_data

if __name__ == '__main__':

    bucket = 'virtual-data-layer'
    input_sql = """select c_custkey,
             c_acctbal,
             count(*)
             from tcph.customer
             where c_custkey <= 16252'
             group by c_custkey
             order by c_cust_key"""

    '''             where c_custkey = '16252'
             or c_custkey = '1777'''

    import sql_to_tree

    sql_tree = sql_to_tree.sql_to_tree(input_sql)
    for k, v in sql_tree.items():
        print(str(k) + ": " + str(v))
    result = exectute_sqltree_on_s3(bucket, sql_tree)
    print(result)
