#!/usr/bin/python
#import oracle_to_postgres
import sql_to_tree
import tree_to_postgres_sql
import psycopg2
import cx_Oracle

def convert_oracle_to_postgres(input_sql,original_datastore_type,target_datastore_type):
    
    if target_datastore_type.upper() == 'POSTGRES':
        sql_tree = sql_to_tree.sql_to_tree(input_sql)
        return tree_to_postgres_sql.tree_to_postgres_sql(sql_tree,original_datastore_type)
    else:
        return 'Datastore not supported!'
    
def connect_to_database(target_datastore_type,target_datastore_url,target_datastore_dbname, target_datastore_username, target_datastore_password):
    #connect to target database
    
    if target_datastore_type == 'postgres':
        conn_string = "host='"+target_datastore_url+"' dbname='"+target_datastore_dbname+"' user='"+target_datastore_username+"' password='"+target_datastore_password+"'"
    
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
 
        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
    
        return cursor
    
    elif target_datastore_type == 'oracle':
        conn_string = target_datastore_username+'/'+target_datastore_password+"@"+target_datastore_url+"/"+target_datastore_dbname
        conn = cx_Oracle.connect(conn_string)
        print(conn.version)
        conn.close
        
        return None
        

def run_on_postgres(postgres_sql, cursor):
    cursor.execute(postgres_sql)
    return cursor.fetchall()
    
      
   