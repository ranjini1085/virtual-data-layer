#!/usr/bin/python
import oracle_virtual_sql
import datetime
import get_credentials

postgres_credentials = get_credentials.get_credentials('postgres_connection.txt')
oracle_credentials = get_credentials.get_credentials('oracle_connection.txt')

#Oracle Unit Test suite
#unit test - connect to postgres DB
#sql_input = 'select c_name from tcph.customer;'
#expected_sql_output = 'select c_name from tcph.customer;'
#try:
#    assert(oracle_virtual_sql.connect_to_database('oracle',oracle_credentials['database_url'],oracle_credentials['database_name'],oracle_credentials['username'],oracle_credentials['password']))
#    print('Oracle connection unit test passed')
#except:
#    print('Oracle connection unit test failed')
#    oracle_virtual_sql.connect_to_database('oracle',oracle_credentials['database_url'],oracle_credentials['database_name'],oracle_credentials['username'],oracle_credentials['password'])

#Postgres Unit Test suite
#unit tests sourced from: https://wiki.postgresql.org/wiki/Oracle_to_Postgres_Conversion

#unit test - convert_oracle_to_postgres Oracle sysdate to postgres [db_sysdate]

sql_input = 'select sysdate from dual;'
expected_sql_output = "select 'now'::timestamp from dual;"
try:
    assert(oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres') == expected_sql_output)
    print('sysdate unit test passed')
except:
    print('sysdate unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres'))

#unit test - rownum

#unit test - convert_oracle_to_postgres Oracle rowid to postgres ctid
sql_input = 'select rowid from dual;'
expected_sql_output = 'select ctid from dual;'
try:
    assert(oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres') == expected_sql_output)
    print('rowid unit test passed')
except:
    print('rowid unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres'))

#unit test - sequences
sql_input = 'select sequencename.nextval from dual;'
expected_sql_output = "select nextval('sequencename') from dual;"
try:
    assert(oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres') == expected_sql_output)
    print('rowid unit test passed')
except:
    print('sequence unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres'))

#unit test - decode

#unit test - convert_oracle_to_postgres Oracle nvl to postgres coalesce
sql_input = 'select nvl(hire_date,fire_date) from dual;'
expected_sql_output = 'select coalesce(hire_date,fire_date) from dual;'
try:
    assert(oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres') == expected_sql_output)
    print('nvl unit test passed')
except:
    print('nvl unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres'))

#unit test - subquery in from

#unit test - outer joins

#unit test - numeric types

#unit test - date and time

#unit test - CLOB/BLOB -- include or exclude???
    
#unit test - upper case
    
#unit test - lower case

#unit test - connect to postgres DB
sql_input = 'select c_name from querytest.customer;'
expected_sql_output = 'select c_name from querytest.customer;'
try:
    assert(oracle_virtual_sql.connect_to_database('postgres',postgres_credentials['database_url'],postgres_credentials['database_name'],postgres_credentials['username'],postgres_credentials['password']))
    print('connection unit test passed')
except:
    print('connection unit test failed')

#create postgres cursor to use in later unit tests
postgres_cursor = oracle_virtual_sql.connect_to_database('postgres',postgres_credentials['database_url'],postgres_credentials['database_name'],postgres_credentials['username'],postgres_credentials['password'])

#unit test - simple postgres query

sql_input = 'select c_name from querytest.customer where c_custkey = 1;'
expected_sql_output = 'select c_name from querytest.customer where c_custkey = 1;'
expected_db_output = 'Customer#000000001'

postgres_sql = oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres')
try:
    assert(postgres_sql == expected_sql_output)
    assert(oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][0] == expected_db_output)
    print('simple postgres query unit test passed')
except:
    print('simple postgres query unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ postgres_sql)
    print(' actual db output: '+oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][0])
    
#unit test - sysdate unit test against postgres DB

sql_input = 'select c_name, sysdate from querytest.customer where c_custkey = 1;'
expected_sql_output = "select c_name, 'now'::timestamp from querytest.customer where c_custkey = 1;"
expected_db_output = 'Customer#000000001'
postgres_sql = oracle_virtual_sql.convert_oracle_to_postgres(sql_input,'postgres')
try:
    assert(postgres_sql == expected_sql_output)
    assert(oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][0] == expected_db_output)
    assert(isinstance(oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][1],datetime.datetime))
    print('simple postgres query unit test passed')
except:
    print('simple postgres query unit test failed')
    print(' expected output: '+ expected_sql_output)
    print(' actual_output: '+ postgres_sql)
    print(' actual db output: '+oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][0])
    print(isinstance(oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][1],datetime.datetime))
    print(' actual timestamp type: '+str(type(oracle_virtual_sql.run_on_postgres(postgres_sql,postgres_cursor)[0][1])))