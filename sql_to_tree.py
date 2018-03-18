#!/usr/bin/python
#import os
#import sys
import sqlparse
import extract_table_names
#import re

def sql_to_tree(input_sql):
    '''Converts SQL to a SQL tree.  Does not validate that input SQL is correct.

    keyword args:
        input_sql: SQL text input.  May be multiple lines, so long as they are separated by semicolon
    
    
    returns:
        list of sql tree components
    '''
    
    sql_tree = {}
    
    sql_tree['tables'] = extract_table_names.extract_tables(input_sql)
    
    return sql_tree

input_sql = """
    select K.a,K.b from (select H.b from (select G.c from (select F.d from
    (select E.e from A, B, C, D, E), F), G), H), I, J, K order by 1,2;
    """

print(sql_to_tree(input_sql))