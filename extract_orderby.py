#!/usr/bin/env python

import sqlparse
from sqlparse.sql import IdentifierList
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
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                if not (identifier.ttype is Whitespace or
                        identifier.ttype is Newline
                        or identifier.ttype is Punctuation):
                            column_identifier = {}
                            column_identifier['column_name'] =\
                                identifier.get_real_name()
                            column_identifier['table_or_alias_name'] =\
                                identifier.get_parent_name()
                            yield column_identifier
        else:
            if not (item.ttype is Whitespace or item.ttype is Newline
                    or item.ttype is Punctuation):
                column_identifier = {}
                column_identifier['column_name'] =\
                    item.get_real_name()
                column_identifier['table_or_alias_name'] =\
                    item.get_parent_name()
                yield column_identifier


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
        l_shipdate <= date '1998-12-01' - interval '90' day (3)
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;"""

    print(extract_orderby(input_sql))
