#!/usr/bin/env python

import sqlparse
from sqlparse.sql import IdentifierList, Function
from sqlparse.tokens import Whitespace, Newline, Punctuation


def extract_orderby_part(parsed):
    '''generator function that extracts "order by" part of a query,
        including any nested subqueries

        keyword-args:
            parsed - list of output from sqlparse.parse() command

        returns:
            each "order by" portion of the query until no more remain
    '''
    order_seen = False
    order_by_seen = False

    for item in parsed.tokens:
        if item.value.upper() == 'HAVING':
            raise StopIteration
        if order_by_seen is True:
            yield item
        if item.value.upper() == 'ORDER':
            order_seen = True
        if order_seen is True and item.value.upper() == 'BY':
            order_by_seen = True


def extract_orderby_identifiers(token_stream):
    '''extracts column identifiers from a "order by" portion of a query
        does not currently support aliased tables -
            will return the alias instead of the table name

        keyword-args:
            token_stream - stream of tokens generated
                by the extract_from_part function

        returns:
            list of column identifiers
    '''

    for item in token_stream:
        identifier_list = []

        # put identifiers into a list if the parser doesn't
        if isinstance(item, IdentifierList):
            identifier_list = item.get_identifiers()
        else:
            identifier_list.append(item)

        for identifier in identifier_list:
            column_identifier = {}

            try:
                # handle functions in order by
                if isinstance(identifier, Function):
                    column_identifier['function'] = identifier.get_real_name()
                    if len(identifier.get_parameters()) > 0:
                        for item in identifier.get_parameters():
                            column_identifier['column_name'] =\
                                item.get_real_name()
                            column_identifier['table_or_alias_name'] =\
                                item.get_parent_name()

                    # handle functions with no column names, like count(*)
                    else:
                        column_identifier['column_name'] = None
                        column_identifier['table_or_alias_name'] = None
                    yield column_identifier

                # handle columns in order by
                elif not (identifier.ttype is Whitespace or
                          identifier.ttype is Newline
                          or identifier.ttype is Punctuation):
                            column_identifier['column_name'] =\
                                identifier.get_real_name()
                            column_identifier['table_or_alias_name'] =\
                                identifier.get_parent_name()
                            column_identifier['function'] = None
                            yield column_identifier
            except AttributeError as ae:
                print("'" + identifier.value + "': " + str(ae))


def extract_orderby(sql):
    '''extracts order by column identifiers from a SQL statement.
        does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of order by column identifiers
    '''
    stream = extract_orderby_part(sqlparse.parse(sql)[0])
    return list(extract_orderby_identifiers(stream))


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

    print(extract_orderby(input_sql))
