import csv
from datetime import datetime


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
        column headers: a dictionary with a mapping between column column_names
                        and column datatypes
        file_data: a list of rows from the local file, with each row
                        represented as a list
'''

    file_data = []
    column_positions = {}
    column_datatypes = {}
    data_filter_sql_tree = []
    data_filter_this_file = {}
    header_filename = local_filename + '_header'

    if sql_tree is not None and 'filters' in sql_tree:
        data_filter_sql_tree = sql_tree['filters']

    lfile_reader = csv.reader(open(local_filename, newline=''),
                              delimiter=',', quotechar='"')

    lfile_header_reader = csv.reader(open(header_filename, newline=''),
                                     delimiter=',', quotechar='"')

    column_names = lfile_reader.__next__()
    column_headers = lfile_header_reader.__next__()

    for i, v in enumerate(column_names):
        column_positions[v] = i

    # set simplified datatype for each column
    for i, v in enumerate(column_headers):
        column_definition = v.split(' ')
        column_name = column_definition[0]
        column_datatype = column_definition[1]
        if column_datatype.upper() == 'DATE':
            column_datatypes[column_name] = 'DATE'

        elif column_datatype.upper() == 'INTEGER':
            column_datatypes[column_name] = 'NUMBER'

        elif column_datatype.upper() == 'NUMERIC':
            column_datatypes[column_name] = 'NUMBER'

        elif 'CHAR' in column_datatype.upper():
            column_datatypes[column_name] = 'CHAR'

        else:
            column_datatypes[column_name] = column_datatype.upper()

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
            filter_values['identifier'] = filter_identifier
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

                    if column_datatypes[filter['identifier']] == 'NUMBER':
                        data_element = float(line_data[filter_position])
                        filter_element = \
                            float(filter['value'].replace("'", ''))
                    elif column_datatypes[filter['identifier']] == 'DATE':
                        data_element = \
                            datetime.strptime(line_data[filter_position],
                                              "%Y-%m-%d")
                        filter_element = \
                            datetime.strptime(filter['value'].replace("'", ''),
                                              "%Y-%m-%d")
                    else:
                        data_element = line_data[filter_position]
                        filter_element = filter['value'].replace("'", '')

                    if filter['operator'] == '=' \
                            and data_element == filter_element:
                        file_data.append(line_data)

                    elif filter['operator'] == '!=' \
                            and data_element != filter_element:
                        file_data.append(line_data)

                    elif filter['operator'] == '>' \
                            and data_element > filter_element:
                        file_data.append(line_data)

                    elif filter['operator'] == '>=' \
                            and data_element >= filter_element:
                        file_data.append(line_data)

                    elif filter['operator'] == '<' \
                            and data_element < filter_element:
                        file_data.append(line_data)

                    elif filter['operator'] == '<=' \
                            and data_element <= filter_element:
                        file_data.append(line_data)

    return column_positions, column_datatypes, file_data


def map_select_columns_to_data(sql_tree, table_name,
                               column_positions, column_datatypes):
    '''takes a sql tree and a given table name and maps the selected columns
        to the data in the table

        keyword_args:
            sql_tree - a sql tree, as generated by sql_to_tree library
            table_name - the name of the table in the sql tree for which
                         the columns will be mapped
            columns_positions - dictionary of column names and their positions
                                in the S3 file
            column_datatypes - dictionary of column names and their datatypes

        returns:
            selected_column - a dictionary of columns mapped to input files,
                              corresponding with the columns selected by
                              this query
                              key is column name
                              value is a tuple of filename and column position

            selected_column_datatype - a dictionary of columns and
                                       their datatypes
                                       key is column name
                                       value is datatype

            join_column - a dictionary of columns mapped to input files,
                              corresponding with the columns involved in
                              this query's joins
                              key is column name
                              value is a tuple of filename and column position

'''
    selected_column = {}
    join_column = {}
    selected_column_datatype = {}
    select_identifiers = sql_tree['select']
    select_identifiers.extend(sql_tree['select aggregate'])
    join_identifiers = sql_tree['joins']

    for i, v in enumerate(select_identifiers):
        if v['column_name'] is not None:
            column_name = v['column_name']
        if column_name in column_positions.keys():
            selected_column[column_name] = \
                (table_name, column_positions[column_name])
            selected_column_datatype[column_name] = \
                column_datatypes[column_name]

    for i, v in enumerate(join_identifiers):
        if v['left_identifier'] in column_positions.keys():
            join_column[v['left_identifier']] = \
                (table_name, column_positions[v['left_identifier']])
        if v['right_identifier'] in column_positions.keys():
            join_column[v['right_identifier']] = \
                (table_name, column_positions[v['right_identifier']])

    return selected_column, selected_column_datatype, join_column


def transpose_columns_to_rows(selected_data_in_columns):
    '''transposes a columnar dataset to a row-based dataset

    keyword_args:
        selected_data_in_columns - a columnar dataset, expressed as a dict
                                  each dictionary item should have the column
                                  name as its key, and a list of the data
                                  for that column as its value

    returns:
        selected_data_headers - list of each of the selected columns
        selected_data_in_rows - list of lists, with each list being a row
                                of data.  first row is the header

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


def column_intersection(left_column, right_column):
    '''determines the intersection between two columns of data
       used in table join

        keyword_args:
            left_column - list, corresponding to left column in join
            right_column - list, corresponding to right column in joins

        returns:
            intersection - dictionary with left column rows as keys,
                           right column rows as values
'''

    hashtable = {}
    intersection = {}

    # bulid a hashtable out of the right column
    for i, v in enumerate(right_column):
        try:
            hashtable[v].append(i)
        except KeyError:
            hashtable[v] = [i]

    # iterate the left column over the hashtable and record results
    for i, v in enumerate(left_column):
        if v in hashtable.keys():
            try:
                intersection[i].append(hashtable[v])
            except KeyError:
                intersection[i] = hashtable[v]

    return intersection


def optimize_intersection_order(sql_tree, join_columns):
    '''optimizes order of tables intersections
       useful when joining multiple tables

       can be optimized much further than the implementation below, which
       only gets the tables in a logical order without taking into account
       size or statistics

       keyword_args:
        sql_tree - a parsed SQL tree, as generated by the sql_to_tree module
        join_columns - a list of the columns in the sql tree's joins, mapped
                       to the tables in the join

      returns:
        ordered_join_list - a reordered 'joins' portion of the sql tree in
        the order that the join should follow
'''

    join_tables_list = []
    join_order = []
    join_list = []
    ordered_join_list = []

    # get pairs of tables to be joined from sql tree and join column maps
    # also, attach tables to be joined to sql tree joins in join_list
    for i, v in enumerate(sql_tree['joins']):
        join = v
        join_tables = []
        left_identifier = join['left_identifier']
        right_identifier = join['right_identifier']

        if left_identifier in join_columns.keys():
            join_tables.append(join_columns[left_identifier][0])
        if right_identifier in join_columns.keys():
            join_tables.append(join_columns[right_identifier][0])

        join['join_tables'] = join_tables

        join_tables_list.append(join_tables)
        join_list.append(join)

    # order joins such that each join features a table in the preceding joins
    # could be optimized to go from smallest table to largest table or
    # something like that
    for i in range(len(join_tables_list)):
        for j, join_tables in enumerate(join_tables_list):
            if len(join_order) == 0:
                join_order.append(join_tables)
            else:
                if set(join_order[i - 1]).intersection(set(join_tables)) and \
                        join_tables not in join_order:
                    join_order.append(join_tables)

    # build ordered_join_list from join_list and join_order
    for i, table_pair in enumerate(join_order):
        for j, join in enumerate(join_list):
            if table_pair == join['join_tables']:
                ordered_join_list.append(join)

    return ordered_join_list


if __name__ == '__main__':
    # unit tests
    left_column = [1, 1, 3, 4, 2]
    right_column = [2, 4, 6, 7, 1, 1]

    sql_tree = {}
    sql_tree['joins'] = [{'left_identifier': 'l_lineitemkey',
                          'right_identifier': 'r_rosterkey', 'join_type': ''},
                         {'left_identifier': 'c_custkey',
                          'right_identifier': 'o_custkey', 'join_type': ''},
                         {'left_identifier': 'l_orderkey',
                          'right_identifier': 'o_orderkey', 'join_type': ''}]

    join_columns = {'c_custkey': ('customer', 0),
                    'o_custkey': ('orders', 1),
                    'o_orderkey': ('orders', 2),
                    'l_orderkey': ('lineitem', 2),
                    'l_lineitemkey': ('lineitem', 3),
                    'r_rosterkey': ('roster', 1)}

    # print(column_intersection(left_column, right_column))
    print(optimize_intersection_order(sql_tree, join_columns))
