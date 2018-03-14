#!/usr/bin/python
import oracle_to_postgres

def convert(sql_input,target_datastore):
    
    if target_datastore == 'postgres':
        return oracle_to_postgres.oracle_to_postgres(sql_input)
    else:
        return 'Datastore not supported!'
    
        
        
   