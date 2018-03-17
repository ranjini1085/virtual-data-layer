#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This example is based on of python-sqlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause
#
# This example illustrates how to extract selected fields from nested
# SELECT statements.
#

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token
from sqlparse.tokens import Keyword, DML


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    select_seen = False
    keyword_count = 0
    for item in parsed.tokens:
        if select_seen and keyword_count == 0:
            if item.ttype is Keyword:
                keyword_count += 1
            else:
                yield item
        elif item.ttype is DML and item.value.upper() == 'SELECT':
            select_seen = True
        elif select_seen and keyword_count > 0:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            else:
                yield None


def extract_selected_fields(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.value
        elif isinstance(item, Token) and item.value != ' ':
            yield item.value


def extract_select(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_selected_fields(stream))


#if __name__ == '__main__':
#    sql = """
#    select K.a,K.b from (select H.b from (select G.c from (select F.d from
#    (select E.e from A, B, C, D, E), F), G), H), I, J, K order by 1,2;
#    """

#    sql= """select a, sum(b) from (select c,d from dual) group by a"""

#    print(extract_select(sql))
#    tables = ', '.join(extract_select(sql))
#    print(tables)
 #   print('Tables: {0}'.format(tables))