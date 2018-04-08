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
            # make sure that this is a filter and not a join or subquery
            if not ((isinstance(item.left, Identifier)
                     or isinstance(item.left, Parenthesis))
                    and (isinstance(item.right, Identifier)
                    or isinstance(item.right, Parenthesis))):
                # if left part of filter is an identifier, mark it as such
                # otherwise, mark it as the value in the filter
                if(isinstance(item.left, Identifier)):
                    comparison_identifier['identifier'] = \
                        item.left.value.replace("''", '')
                else:
                    comparison_identifier['value'] = \
                        item.left.value.replace("''", '')
                # if right part of filter is an identifier, mark it as such
                # otherwise, mark it as the value in the filter
                if(isinstance(item.right, Identifier)):
                    comparison_identifier['identifier'] = \
                        item.right.value.replace("''", '')
                else:
                    comparison_identifier['value'] = \
                        item.right.value.replace("''", '')

                # sqlparse doesn't have a function to extract the operator,
                # so we will do it by removing the other parts of the filter
                operator = item.value.replace(item.left.value, '')
                operator = operator.replace(item.right.value, '')
                operator = operator.replace(' ', '')

                # subsequent uses of this will assume value is on the right
                # so if value is on the left, reverse operator
                if not (isinstance(item.left, Identifier)):
                    if '>' in operator:
                        operator = operator.replace('>', '<')
                    elif '<' in operator:
                        operator = operator.replace('<', '>')
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
        join_identifier = {}
        if isinstance(item, Comparison):
            if isinstance(item.left, Identifier) \
                    and isinstance(item.right, Identifier):
                join_identifier['left_identifier'] = item.left.value
                join_identifier['right_identifier'] = item.right.value
                join_identifier['join_type'] = ''

                yield(join_identifier)


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
            l_orderkey,
            sum(l_extendedprice),
            o_orderdate,
            o_shippriority
        from
            tcph.customer,
            tcph.orders,
            tcph.lineitem
        where
            c_mktsegment = 'BUILDING'
            and c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate < '1997-12-31'
            and l_shipdate > '1998-01-01'
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
            order by
            sum(l_extendedprice),
            o_orderdate;"""

    print('joins: '+str(extract_joins(input_sql)))
    print('filters: '+str(extract_filters(input_sql)))
    print('subselects: '+str(extract_where_subqueries(input_sql)))
