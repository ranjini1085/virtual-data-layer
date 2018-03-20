#!/usr/bin/env python

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

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
#    sql = """
#    select K.a,K.b from (select H.b from (select G.c from (select F.d from
#    (select E.e from A as first, B, C, D, E), F), G), H), I, J, K order by 1,2;
#    """

    sql = """select c.customer_name, o.order_date
            from tcph.customer c, tcph.order o
            where c.customer_id = o.customer_id
            and c.customer_id = (select customer_id from orders o where order_id =1)"""

    sql = """select c.customer_name, o.order_date
        from (select customer_id from customer where customer_id = 1) c, tcph.order o
        where c.customer_id = o.customer_id"""

    for i in sqlparse.parse(sql)[0]:
        print(i)
        print(is_subselect(i))
        
    #return list(extract_table_identifiers(stream))



 #   print(extract_tables(sql))