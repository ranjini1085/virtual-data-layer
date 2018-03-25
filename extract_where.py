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
            if not ((isinstance(item.left, Identifier)
                     or isinstance(item.left, Parenthesis))
                    and (isinstance(item.right, Identifier)
                    or isinstance(item.right, Parenthesis))):
                yield (item.left.value, item.right.value)


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
    sql = """select c.customer_name, o.order_date
            from tcph.customer c, tcph.order o
            where o.order_id = (select order_id from orders where order_id = 1)
            and c.customer_name = 'Tony'
        """

    print('joins: '+str(extract_joins(sql)))
    print('filters: '+str(extract_filters(sql)))
    print('subselects: '+str(extract_where_subqueries(sql)))
