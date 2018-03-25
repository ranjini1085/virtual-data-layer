#!/usr/bin/env python

import sqlparse
from sqlparse.tokens import Whitespace, Newline, Punctuation


def extract_groupby_part(parsed):
    '''generator function that extracts "group by" part of a query,
                                including any nested subqueries

        keyword-args:
            parsed - list of output from sqlparse.parse() command

        returns:
            each "group by" portion of the query until no more remain
    '''
    where_seen = False
    group_by_seen = False

    for item in parsed.tokens:
        if item.value.upper() == 'ORDER' or item.value.upper() == 'HAVING':
            raise StopIteration
        if group_by_seen is True and item.value.upper() != 'BY':
            yield item
        if where_seen is True and item.value.upper() == 'GROUP':
            group_by_seen = True
        if isinstance(item, sqlparse.sql.Where):
            where_seen = True


def extract_groupby_identifiers(token_stream):
    '''extracts column identifiers from a "group by" portion of a query
        does not currently support aliased tables -
                will return the alias instead of the table name

        keyword-args:
            token_stream - stream of tokens generated
                        by the extract_from_part function

        returns:
            list of column identifiers
    '''

    for item in token_stream:
        if not (item.ttype is Whitespace
                or item.ttype is Newline or item.ttype is Punctuation):
            yield item.value


def extract_groupby(sql):
    '''extracts group by column identifiers from a SQL statement.
            does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of group by column identifiers
    '''
    stream = extract_groupby_part(sqlparse.parse(sql)[0])
    return list(extract_groupby_identifiers(stream))


if __name__ == '__main__':
    sql = """select c.customer_name, o.order_date, sum(order_count)
            from tcph.customer c, tcph.order o
            where o.order_id = (select order_id from orders where order_id = 1)
            group by c.customer_name, o.order_date
            order by c.customer_name desc, o.order_date asc
            having sum(order_count) > 1"""

    print(extract_groupby(sql))
