#!/usr/bin/env python

import sqlparse
from sqlparse.tokens import Whitespace, Newline, Punctuation


def extract_having_part(parsed):
    '''generator function that extracts "having" part of a query,
        including any nested subqueries

        keyword-args:
            parsed - list of output from sqlparse.parse() command

        returns:
            each "having" portion of the query until no more remain
    '''
    having_seen = False

    for item in parsed.tokens:
        if having_seen is True:
            yield item
        if item.value.upper() == 'HAVING':
            having_seen = True


def extract_having_identifiers(token_stream):
    '''extracts aggregate identifiers from a "having" portion of a query
        does not currently support aliased tables

        keyword-args:
            token_stream - stream of tokens generated
                by the extract_from_part function

        returns:
            list of aggregate identifiers
    '''

    for item in token_stream:
        if not (item.ttype is Whitespace or item.ttype is Newline
                or item.ttype is Punctuation):
            yield item.value


def extract_having(sql):
    '''extracts having aggregate identifiers from a SQL statement.
        does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement

        returns: a list of having aggregate identifiers
    '''
    stream = extract_having_part(sqlparse.parse(sql)[0])
    return list(extract_having_identifiers(stream))


if __name__ == '__main__':

    sql = """select c.customer_name, o.order_date
            from tcph.customer c, tcph.order o
            where o.order_id = (select order_id from orders where order_id = 1)
            group by c.customer_name, o.order_date
            order by c.customer_name desc, o.order_date asc
            having sum(orders) > 2, count(orders) < 10"""

    print(extract_having(sql))
