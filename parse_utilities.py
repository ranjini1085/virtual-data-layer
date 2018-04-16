#!/usr/bin/env python

import sqlparse
from sqlparse.tokens import DML


def is_subselect(parsed):
    '''determines if a sqlparse Item is a subselect part of a SQL query

        keyword args:
            parsed - a sqlparse Item

        returns:
            True if Item is a subselect, false if not
    '''

    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


if __name__ == '__main__':

    sql = """select c.customer_name, o.order_date
        from customer c, order o
        where c.customer_id = o.customer_id
        order by c.customer_name"""

    for i in sqlparse.parse(sql)[0]:
        print(i)
        print(type(i))
