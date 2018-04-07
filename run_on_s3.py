#!/usr/bin/python
import boto3
from botocore.exceptions import EndpointConnectionError
from file_query_utilities import file_to_data_structure,\
                           map_select_columns_to_data,\
                           transpose_columns_to_rows


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

    local_header_filename = local_filename + '_header'
    s3_header_filename = s3_filename + '_header'


#   download file
    try:
        with open(local_filename, "wb") as s3_file:
            s3.download_fileobj(bucket, s3_filename, s3_file)

        with open(local_header_filename, "wb") as s3_file:
            s3.download_fileobj(bucket, s3_header_filename, s3_file)
    except EndpointConnectionError:
        print('cannot connect to S3 bucket')

#   return name of local file
    return local_filename


def execute_sqltree_on_s3(bucket, sql_tree):
    '''executes a SQL Tree against a S3 bucket

    keyword-args:
        bucket - name of S3 bucket that the query will be executed against
        sql_tree - a sql tree, as generated by sql_to_tree library

    returns:
        ordered_data - a dataset expressed as a list of lists
                        each list is a row, and the first row is a header

'''
    query_data = {}
    query_data_column_positions = {}
    query_data_headers = {}
    selected_columns = []
    selected_columns_datatypes = []
    selected_data = {}
    post_join_row_count = 0

    # download files in query and map to data structures
    for table_definition in sql_tree['table_definitions']:
        alias = ''
        s3_folder = table_definition['schema']
        s3_filename = table_definition['name']
        if table_definition['alias'] is not None:
            alias = table_definition['alias']
        else:
            alias = table_definition['name']

        query_data_column_positions[alias], query_data_headers[alias], \
            query_data[alias] = \
            file_to_data_structure(retrieve_s3_file(bucket,
                                                    s3_filename, s3_folder),
                                   sql_tree)

    # map selected columns to tables
    for k in query_data_column_positions:
            mapped_columns, mapped_headers = \
                map_select_columns_to_data(sql_tree, k,
                                           query_data_column_positions[k],
                                           query_data_headers[k])

            selected_columns.append(mapped_columns)
            selected_columns_datatypes.append(mapped_headers)

    # merge list of selected columns datatypes into one dictionary
    selected_columns_datatypes = {k: v for d in selected_columns_datatypes
                                  for k, v in d.items()}

    # select columns from specified dataset
    for i, column in enumerate(selected_columns):
        for k in column.keys():
            selected_data[k] = []
            select_table = column[k][0]
            selected_column_position = column[k][1]

            for row in query_data[select_table]:
                selected_data[k].append(row[selected_column_position])

    # apply joins

    # get length of resulting dataset
    for k in selected_data.keys():
        column_length = len(selected_data[k])
        if column_length > post_join_row_count:
            post_join_row_count = column_length

    # apply aggregates and having
    if len(sql_tree['grouping']) > 0:
        grouping_columns = []
        grouping_rows = {}

        # step 1: map group bys to columns
        for i, grouping_item in enumerate(sql_tree['grouping']):
            try:
                column_name = grouping_item['column_name']
                grouping_columns.append(column_name)
            except KeyError as e:
                print(e)

        # step 2: get distint values from group by columns
        #   and denote the rows for each distinct value
        for row in range(post_join_row_count):
            unique_grouping = []
            try:
                for k, column in enumerate(grouping_columns):
                    unique_grouping.append(selected_data[column][row])
            except KeyError as e:
                print(e)

            if repr(unique_grouping) not in grouping_rows.keys():
                grouping_rows[repr(unique_grouping)] = []

            grouping_rows[repr(unique_grouping)].append(row)

        # setp 3: build base dataset from unique groupings
        aggregated_data = {}

        for i, v in enumerate(grouping_columns):
            aggregated_data[grouping_columns[i]] = []

        for k in grouping_rows.keys():
            unique_record = k.strip("[]'")
            for i, v in enumerate(unique_record.split(',')):
                aggregated_data[grouping_columns[i]].append(v)

        # step 4: aggregate all rows for each distinct value combination
        if len(sql_tree['select aggregate']) > 0:
            for i, aggregate in enumerate(sql_tree['select aggregate']):
                if aggregate['column_name'] is not None:
                    aggregate_name = aggregate['function'] + \
                        '_' + aggregate['column_name']
                else:
                    aggregate_name = aggregate['function']

                aggregated_data[aggregate_name] = []

                for unique_record, rows in grouping_rows.items():
                    if aggregate['column_name'] is not None:
                        group_list = []
                        for i, row in enumerate(rows):
                            group_list.append(
                                selected_data[aggregate['column_name']][row])

                        # build functions to deal with type conversions for
                        # numbers and dates
                        aggregate_func = ''
                        if selected_columns_datatypes[
                                aggregate['column_name']] == 'NUMBER':
                            aggregate_func = \
                                eval('[float(i) for i in group_list]')
                        elif selected_columns_datatypes[
                                aggregate['column_name']] == 'DATE':
                            aggregate_func = \
                                eval('[datetime.strptime(i, "%Y-%m-%d") ' +
                                     'for i in group_list]')
                        else:
                            aggregate_func = eval('group_list')
                        try:
                            if aggregate['function'] == 'count':
                                aggregated_data[aggregate_name].append(
                                    str(len(set(group_list))))

                            elif aggregate['function'] == 'sum':
                                aggregated_data[aggregate_name].append(
                                    str(sum(aggregate_func)))

                            elif aggregate['function'] == 'max':
                                aggregated_data[aggregate_name].append(
                                    str(max(aggregate_func)))
                                print(type(aggregate_func))

                            elif aggregate['function'] == 'min':
                                aggregated_data[aggregate_name].append(
                                    str(min(aggregate_func)))

                            elif aggregate['function'] == 'avg':
                                aggregated_data[aggregate_name].append(
                                    str(sum(aggregate_func)
                                        / len(group_list)))
                        except TypeError as te:
                            print('SQL Error: ' + str(te))
                            return 'SQL Error'

                    elif aggregate['column_name'] is None and \
                            aggregate['function'] == 'count':
                        aggregated_data[aggregate_name].append(
                            str(len(set(rows))))

        selected_data = aggregated_data

    # move from columnar to row orientation and apply order by

    # add support for ordering asc or desc
    # add support for column definitions so we can determine how to sort
    # assumes sorting on numbers for now

    selection_headers, selected_rows = transpose_columns_to_rows(selected_data)
    ordered_data = [selection_headers]

    # only apply the sort if there are fields in the
    #   ordering part of the sql tree
    if len(sql_tree['ordering']) > 0:
        # build a lambda function for the sort
        order_fields_func = 'lambda i: ('
        for i, order_item in enumerate(sql_tree['ordering']):

            # handle function vs. column
            if order_item['function'] is None:
                order_column_name = order_item['column_name']
            else:
                order_column_name = order_item['function'] + '_' + \
                                    order_item['column_name']

            if order_column_name in selection_headers:

                # if type is number, convert to float in lambda function
                # use the original column name when looking up the datatype,
                # but use the derived column name when building the header,
                # as it's important to use the aggregate name in output
                if selected_columns_datatypes[order_item['column_name']] \
                        == 'NUMBER':
                    sort_item = 'float(i[' + \
                     str(selection_headers.index(order_column_name)) + ']),'

                # if type is date, convert to datetime in lambda function
                elif selected_columns_datatypes[order_item['column_name']] \
                        == 'DATE':
                    sort_item = 'datetime.strptime(i[' + \
                     str(selection_headers.index(order_column_name)) + \
                     '], "%Y-%m-%d"),'

                # else treat as a string
                else:
                    sort_item = 'i[' + \
                        str(selection_headers.index(order_column_name)) + '],'

                order_fields_func += sort_item

        order_fields_func = order_fields_func.rstrip(',')
        order_fields_func += ')'
        order_fields_func = eval(order_fields_func)

        # sort based on fields in lambda function
        ordered_data.extend(sorted(selected_rows, key=order_fields_func))
    # otherwise, just return unsorted rows
    else:
        ordered_data.extend(selected_rows)

    return ordered_data


if __name__ == '__main__':

    bucket = 'virtual-data-layer'
    input_sql = """
                select
                    c_custkey,
                    o_orderdate,
                    o_shippriority
            from
                tcph.customer,
                tcph.orders
            where
                c_mktsegment = 'BUILDING'
                and c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate < '1997-12-31'"""

    import sql_to_tree

    sql_tree = sql_to_tree.sql_to_tree(input_sql)
    # for k, v in sql_tree.items():
    #     print(str(k) + ": " + str(v))
    result = execute_sqltree_on_s3(bucket, sql_tree)
    print(result)
