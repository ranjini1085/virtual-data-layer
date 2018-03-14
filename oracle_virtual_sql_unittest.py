#!/usr/bin/python
import oracle_virtual_sql

#Postgres Unit Test suite
#unit tests sourced from: https://wiki.postgresql.org/wiki/Oracle_to_Postgres_Conversion

#unit test - convert Oracle sysdate to postgres [db_sysdate]

sql_input = 'select sysdate from dual;'
expected_sql_output = 'select [db_sysdate] from dual;'
try:
    assert(oracle_virtual_sql.convert(sql_input,'postgres') == expected_sql_output)
    print('sysdate unit test passed')
except:
    print('sysdate unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert(sql_input,'postgres'))

#unit test - rownum

#unit test - convert Oracle rowid to postgres ctid
sql_input = 'select rowid from dual;'
expected_sql_output = 'select ctid from dual;'
try:
    assert(oracle_virtual_sql.convert(sql_input,'postgres') == expected_sql_output)
    print('rowid unit test passed')
except:
    print('rowid unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert(sql_input,'postgres'))

#unit test - sequences

#unit test - decode

#unit test - convert Oracle nvl to postgres coalesce
sql_input = 'select nvl(hire_date,fire_date) from dual;'
expected_sql_output = 'select coalesce(hire_date,fire_date) from dual;'
try:
    assert(oracle_virtual_sql.convert(sql_input,'postgres') == expected_sql_output)
    print('nvl unit test passed')
except:
    print('nvl unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert(sql_input,'postgres'))

#unit test - subquery in from

#unit test - outer joins

#unit test - numeric types

#unit test - date and time

#unit test - CLOB/BLOB -- include or exclude???
    
#unit test - upper case
    
#unit test - lower case

