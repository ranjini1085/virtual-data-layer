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