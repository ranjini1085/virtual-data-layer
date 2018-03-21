#!/usr/bin/python
#import os
#import sys
import sqlparse
import extract_table_names
import extract_selected_columns
import extract_where
import extract_orderby
import extract_aggregates
#import re

def sql_to_tree(input_sql):
    '''Converts SQL to a SQL tree.  Does not validate that input SQL is correct.

    keyword args:
        input_sql: SQL text input.  May be multiple lines, so long as they are separated by semicolon
    
    
    returns:
        list of sql tree components
    '''
    
    sql_tree = {}
    
    sql_tree['select'] = extract_selected_columns.extract_select(input_sql) #remove aggregates
#    sql_tree['select aggregate'] = need to add aggregates
    sql_tree['tables'] = extract_table_names.extract_tables(input_sql) #needsto support aliases
    sql_tree['joins'] = extract_where.extract_joins(input_sql)
    sql_tree['filters'] = extract_where.extract_filters(input_sql)
    sql_tree['where_subqueries'] = extract_where.extract_where_subqueries(input_sql) #need to split out join from subquery
    sql_tree['grouping'] = extract_aggregates.extract_groupby(input_sql)
    sql_tree['ordering'] = extract_orderby.extract_orderby(input_sql)
    
    return sql_tree


if __name__ == '__main__':
    input_sql = """select c.customer_name, o.order_date, sum(o.orders)
                from tcph.customer c, tcph.order o
                where c.customer_id = o.customer_id
                and c.customer_id = 1
                group by c.customer_name, o.order_date
                order by c.customer_name;"""

    print(sql_to_tree(input_sql))