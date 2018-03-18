#!/usr/bin/python
#import os
#import sys
import sqlparse
#import re

def oracle_to_postgres(input_sql):
    '''Converts oracle-compatible SQL to postgres-compatible SQL.  Does not validate that input SQL is correct.

    keyword args:
        input_sql: SQL text input.  May be multiple lines, so long as they are separated by semicolon
    
    
    returns:
        postgres-compatible SQL string
    '''