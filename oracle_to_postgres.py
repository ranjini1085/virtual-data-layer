#!/usr/bin/python
import os
import sys
import sqlparse
import re

def oracle_to_postgres(input_sql):
    '''Converts oracle-compatible SQL to postgres-compatible SQL.  Does not validate that input SQL is correct.

    keyword args:
        input_sql: SQL text input.  May be multiple lines, so long as they are separated by semicolon
    
    
    returns:
        postgres-compatible SQL string
    '''
    
    
    sql_statements = sqlparse.split(input_sql)
    postgres_stmt = ''
    
    for i in sql_statements:

        parsed_sql = sqlparse.parse(i)

        for j in parsed_sql:
            stmt = j

            #first, do the simple token replacements using replacements and regexes
            for x in stmt.tokens:
#                print(x.value)
                postgres_token = x.value
                postgres_token = postgres_token.replace('sysdate',"'now'::timestamp")
                postgres_token = postgres_token.replace('nvl(','coalesce(')
                postgres_token = postgres_token.replace('rowid','ctid')
                postgres_token = re.sub(r'(\w+)\.nextval',"nextval('\g<1>')",postgres_token)
                
                postgres_stmt += (postgres_token)
            
            #    if x.value == 'sysdate':
            #        postgres_stmt += '[db_sysdate]'
            #    elif x.value == 'rowid':
            #        postgres_stmt += 'ctid'
            #    elif x.value == 'nvl':
            #        postgres_stmt += 'coalesce'
            #    else:
            #        postgres_stmt += (x.value)

 
#    print postgres_stmt
    return postgres_stmt
