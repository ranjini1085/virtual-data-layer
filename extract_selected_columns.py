#!/usr/bin/env python

import sqlparse
from sqlparse.sql import IdentifierList, Token, Function
from sqlparse.tokens import Keyword, DML, Whitespace, Newline

from parse_utilities import is_subselect


def extract_select_part(parsed):
    '''generator function that extracts "select" part of a query,
        including any nested subqueries

        keyword-args:
            parsed - list of output from sqlparse.parse() command

        returns:
            each "select" portion of the query until no more remain
    '''
    select_seen = False
    keyword_count = 0
    for item in parsed.tokens:
        if select_seen and keyword_count == 0:
            if item.ttype is Keyword:
                keyword_count += 1
            elif item.ttype is Whitespace or item.ttype is Newline:
                yield None
            else:
                yield item
        elif item.ttype is DML and item.value.upper() == 'SELECT':
            select_seen = True
        elif select_seen and keyword_count > 0:
            if is_subselect(item):
                for x in extract_select_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            else:
                yield None


def extract_selected_fields(token_stream):
    '''extracts column identifiers from a "select" portion of a query

        keyword-args:
        token_stream - stream of tokens generated
                        by the extract_select_part function

        returns:
        list of column identifiers
    '''

    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                column_identifier = {}
                if not isinstance(identifier, Function):
                    column_identifier['column_name'] =\
                        identifier.get_real_name()
                    column_identifier['table_or_alias_name'] =\
                        identifier.get_parent_name()
                    yield column_identifier
        elif isinstance(item, Token) and item.value != ' ' \
                and not isinstance(item, Function):
            column_identifier = {}
            column_identifier['column_name'] =\
                item.get_real_name()
            column_identifier['table_or_alias_name'] =\
                item.get_parent_name()
            yield column_identifier


def extract_selected_aggregates(token_stream):
    '''extracts column identifiers from a "select" portion of a query

        keyword-args:
        token_stream - stream of tokens generated
            by the extract_select_part function

        returns:
        list of column identifiers
    '''

    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                column_identifier = {}
                if isinstance(identifier, Function):
                    column_identifier['function'] = identifier.get_real_name()
                    if len(identifier.get_parameters()) > 0:
                        for item in identifier.get_parameters():
                            column_identifier['column_name'] =\
                                item.get_real_name()
                            column_identifier['table_or_alias_name'] =\
                                item.get_parent_name()
                    else:
                        column_identifier['column_name'] = None
                        column_identifier['table_or_alias_name'] = None
                    yield column_identifier
        elif isinstance(item, Token) and isinstance(item, Function):
            column_identifier = {}
            column_identifier['column_name'] =\
                item.get_real_name()
            column_identifier['table_or_alias_name'] =\
                item.get_parent_name()
            yield column_identifier


def extract_select(sql):
    '''extracts selected columns from a SQL statement.  does not validate
        that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of column identifiers
    '''

    stream = extract_select_part(sqlparse.parse(sql)[0])
    return list(extract_selected_fields(stream))


def extract_select_aggregates(sql):
    '''extracts selected columns from a SQL statement.
            does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of column identifiers
    '''

    stream = extract_select_part(sqlparse.parse(sql)[0])
    return list(extract_selected_aggregates(stream))


if __name__ == '__main__':

    input_sql = """
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity),
        sum(l_extendedprice),
        avg(l_quantity),
        avg(l_extendedprice),
        avg(l_discount),
        count(*)
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

    print(extract_select(input_sql))
    print(extract_select_aggregates(input_sql))
