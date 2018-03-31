#!/usr/bin/env python

import sqlparse
from sqlparse.sql import Identifier, Comparison, Parenthesis
from sqlparse.tokens import Keyword, Whitespace, Newline, Punctuation


def extract_where_part(parsed):
    '''generator function that extracts "where" part of a query,
                                including any nested subqueries

        keyword-args:
            parsed - list of output from sqlparse.parse() command

        returns:
            each "where" portion of the query until no more remain
    '''

    for item in parsed.tokens:
        if isinstance(item, sqlparse.sql.Where):
            for where_token in (item):
                if where_token.ttype is not Keyword \
                    and where_token.ttype is not Punctuation \
                    and where_token.ttype is not Whitespace \
                        and where_token.ttype is not Newline:
                            yield where_token


def extract_filter_identifiers(token_stream):
    '''extracts filters identifiers from a "where" portion of a query

        keyword-args:
        token_stream - stream of tokens generated
                        by the extract_select_part function

        returns:
        stream of filter identifiers
    '''

    for item in token_stream:
        if isinstance(item, Comparison):
            comparison_identifier = {}
            if not ((isinstance(item.left, Identifier)
                     or isinstance(item.left, Parenthesis))
                    and (isinstance(item.right, Identifier)
                    or isinstance(item.right, Parenthesis))):
                comparison_identifier['left'] = item.left.value
                comparison_identifier['right'] =\
                    item.right.value.replace("'", '')
                operator = item.value.replace(item.left.value, '')
                operator = operator.replace(item.right.value, '')
                operator = operator.replace(' ', '')
                comparison_identifier['operator'] = operator
                yield comparison_identifier


def extract_join_identifiers(token_stream):
    '''extracts join identifiers from a "where" portion of a query

        keyword-args:
        token_stream - stream of tokens generated
                        by the extract_select_part function

        returns:
        stream of join identifiers
    '''

    for item in token_stream:
        if isinstance(item, Comparison):
            if isinstance(item.left, Identifier) \
                    and isinstance(item.right, Identifier):
                yield(item.value)


def extract_where_subquery_identifiers(token_stream):
    '''extracts subquery identifiers from a "where" portion of a query

        keyword-args:
        token_stream - stream of tokens generated
                    by the extract_select_part function

        returns:
        stream of subquery identifiers
    '''

    for item in token_stream:
        if isinstance(item, Comparison):
            if isinstance(item.left, Parenthesis) \
                    or isinstance(item.right, Parenthesis):
                yield(item.value)


def extract_filters(sql):
    '''extracts join columns from a SQL statement.
            does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of join identifiers
    '''
    stream = extract_where_part(sqlparse.parse(sql)[0])
    return list(extract_filter_identifiers(stream))


def extract_joins(sql):
    '''extracts join columns from a SQL statement.
            does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of join identifiers
    '''
    stream = extract_where_part(sqlparse.parse(sql)[0])
    return list(extract_join_identifiers(stream))


def extract_where_subqueries(sql):
    '''extracts join columns from a SQL statement.
                does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of join identifiers
    '''
    stream = extract_where_part(sqlparse.parse(sql)[0])
    return list(extract_where_subquery_identifiers(stream))


if __name__ == '__main__':
    input_sql = """
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= '1998-12-01'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;"""

    print('joins: '+str(extract_joins(input_sql)))
    print('filters: '+str(extract_filters(input_sql)))
    print('subselects: '+str(extract_where_subqueries(input_sql)))
