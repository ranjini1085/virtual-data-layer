#!/usr/bin/python

def tree_to_ansi_sql(sql_tree):
    '''Converts SQL tree to SQL.  Does not validate that input SQL is correct.  Produces ANSI-compatible SQL

    keyword args:
        input_sql: SQL tree, as generated by sql_to_tree module
    
    
    returns:
        SQL command text string        
    '''
    
    sql_command = ''
    
    #construct select block
    if len(sql_tree['select']) + len(sql_tree['select aggregate']) > 0:
        sql_command += 'select '
    
        select_block = sql_tree['select']
        select_block.extend(sql_tree['select aggregate'])

        for i,v in enumerate(select_block):
            sql_command += v
            if i < len(select_block) - 1:
                sql_command += ', '
    
    #construct from block
    if len(sql_tree['table_aliases']) > 0:
        sql_command += ' from '
    
        from_block = sql_tree['table_aliases']
        for i, table_alias_pair in enumerate(from_block):
            sql_command += table_alias_pair[0]
            if table_alias_pair[1] != None:
                sql_command += ' as '
                sql_command += table_alias_pair[1]
                
            if i < len(from_block) - 1:
                sql_command += ', '
    
    #construct where block
    if len(sql_tree['joins']) + len(sql_tree['where_subqueries']) + len(sql_tree['filters']) > 0:
        sql_command += ' where '
    
        where_block = sql_tree['joins']
        where_block.extend(sql_tree['where_subqueries'])
        where_block.extend(sql_tree['filters'])
        
        for i, join in enumerate(where_block):
            sql_command += join
            
            if i < len(where_block) - 1:
                sql_command += ' and '
                
    #construct group by block
    if len(sql_tree['grouping']) > 0:
        sql_command += ' group by '
    
        groupby_block = sql_tree['grouping']
        
        for i, grouping in enumerate(groupby_block):
            sql_command += grouping
            
            if i < len(groupby_block) - 1:
                sql_command += ', '
            
    #construct order by block
    if len(sql_tree['ordering']) > 0:
        sql_command += ' order by '
        
        orderby_block = sql_tree['ordering']
        
        for i, ordering in enumerate(orderby_block):
            sql_command += ordering
            
            if i < len(orderby_block) - 1:
                sql_command += ', '
                
    #construct having block
    if len(sql_tree['having']) > 0:
        sql_command += ' having '
        
        having_block = sql_tree['having']
        
        for i, having in enumerate(having_block):
            sql_command += having
            
            if i < len(having_block) - 1:
                sql_command += ', '
    
    return sql_command

if __name__ == '__main__':
    
    import sql_to_tree
    
    input_sql = """select c.customer_name, o.order_date, sum(o.orders)
                from tcph.customer as c, tcph.order o, tcph.part
                where c.customer_id = o.customer_id
                and o.part_number = tcph.part.part_number
                and c.customer_id = 1
                group by c.customer_name, o.order_date
                order by c.customer_name;"""

    print(tree_to_ansi_sql(sql_to_tree.sql_to_tree(input_sql)))