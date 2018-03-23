#!/usr/bin/env python

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

import parse_utilities
from parse_utilities import is_subselect

def extract_from_part(parsed):
    '''generator function that extracts "from" part of a query, including any nested subqueries

        keyword-args:
            parsed - list of output from sqlparse.parse() command
            
        returns:
            each "from" portion of the query until no more remain
    '''
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    '''extracts table identifiers from a "from" portion of a query

        keyword-args:
            token_stream - stream of tokens generated by the extract_from_part function
            
        returns:
            list of table identifiers
    '''
    
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_real_name()
        elif isinstance(item, Identifier):
            yield item.get_real_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value

def extract_table_alias_identifiers(token_stream):
    '''extracts table aliases from a "from" portion of a query

        keyword-args:
            token_stream - stream of tokens generated by the extract_from_part function
            
        returns:
            list of tables and their aliases
    '''
    
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield [identifier.get_real_name(),identifier.get_alias()]
        elif isinstance(item, Identifier):
            yield [identifier.get_real_name(),identifier.get_alias()]
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value

def extract_tables(sql):
    '''extracts tables from a SQL statement.  does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement
        
        returns: a list of table identifiers
    '''
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))

def extract_table_aliases(sql):
    '''extracts tables and their aliases from a SQL statement.  does not validate that the SQL is correct

        keyword-args: a string containing a SQL statement
        
        returns: a list of table identifiers
    '''
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_alias_identifiers(stream))

if __name__ == '__main__':
#    sql = """
#    select K.a,K.b from (select H.b from (select G.c from (select F.d from
#    (select E.e from A as first, B, C, D, E), F), G), H), I, J, K order by 1,2;
#    """

    sql = """select c.customer_name, o.order_date
            from tcph.customer c, tcph.order as o, tcph.part
            where c.customer_id = o.customer_id
            and c.customer_id = o.customer_id
            and o.part_number = tcph.part.part_number;"""

    print(extract_tables(sql))
    print(extract_table_aliases(sql))