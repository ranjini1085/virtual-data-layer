#!/usr/bin/python
import re


def syntax_replace_posgres(input_token, sql_type):
    '''Converts oracle-compatible SQL tokens to postgres-compatible SQL tokens.

    keyword args:
        input_token: SQL token text
        sql_type: type of SQL system that tree was originally intended for.
            Currently supports 'Oracle'


    returns:
        SQL string with non-Postgres syntax replaced by
                                        Postgres-compatible syntax
    '''

    sql_token = input_token

    if sql_type.upper() == 'ORACLE':
        sql_token = sql_token.replace('sysdate', "'now'::timestamp")
        sql_token = sql_token.replace('nvl(', 'coalesce(')
        sql_token = sql_token.replace('rowid', 'ctid')
        sql_token = re.sub(r'(\w+)\.nextval', "nextval('\g<1>')", sql_token)

    return sql_token


def tree_to_postgres_sql(sql_tree, sql_type):
    '''Converts SQL tree to Postgres-compatible SQL.
        Does not validate that input SQL is correct.

    keyword args:
        sql_tree: SQL tree, as generated by sql_to_tree module
        sql_type: type of SQL system that tree was originally intended for.
                        Currently supports 'Oracle'


    returns:
        SQL command text string
    '''

    sql_command = ''

    # construct select block
    if len(sql_tree['select']) + len(sql_tree['select aggregate']) > 0:
        sql_command += 'select '

        select_block = sql_tree['select']
        select_block.extend(sql_tree['select aggregate'])

        for i, select_field in enumerate(select_block):

            # assemble select statment
            # functions can be added without modifiation
            # but do we need to look at their syntax as well?
            if 'function' in select_field.keys():
                sql_command += select_field['function']
            else:
                if select_field['table_or_alias_name'] is not None:
                    sql_command += select_field['table_or_alias_name']
                    sql_command += '.'
                # replace non-standard SQL parts with Postgres SQL parts
                sql_command += \
                    syntax_replace_posgres(select_field['column_name'],
                                           sql_type)

            if i < len(select_block) - 1:
                sql_command += ', '

    # construct from block
    if len(sql_tree['table_definitions']) > 0:
        sql_command += ' from '

        from_block = sql_tree['table_definitions']
        for i, table_definition in enumerate(from_block):
            if table_definition['schema'] is not None:
                sql_command += table_definition['schema']
                sql_command += '.'

            sql_command += table_definition['name']

            if table_definition['alias'] is not None:
                sql_command += ' as '
                sql_command += table_definition['alias']

            if i < len(from_block) - 1:
                sql_command += ', '

    # construct where block
    if len(sql_tree['joins']) + len(sql_tree['where_subqueries']) + \
       len(sql_tree['filters']) > 0:
        sql_command += ' where '

        where_block = sql_tree['joins']
        where_block.extend(sql_tree['where_subqueries'])

        for i, v in enumerate(sql_tree['filters']):
            filter = []
            filter.append(v[0] + ' = ' + v[1])
            where_block.extend(filter)

        for i, join in enumerate(where_block):

            # replace non-standard SQL parts with Postgres SQL parts
            sql_command += syntax_replace_posgres(join, sql_type)

            if i < len(where_block) - 1:
                sql_command += ' and '

    # construct group by block
    if len(sql_tree['grouping']) > 0:
        sql_command += ' group by '

        groupby_block = sql_tree['grouping']

        for i, grouping in enumerate(groupby_block):
            if grouping['table_or_alias_name'] is not None:
                sql_command += grouping['table_or_alias_name']
                sql_command += '.'
            # replace non-standard SQL parts with Postgres SQL parts
            sql_command += \
                syntax_replace_posgres(grouping['column_name'],
                                       sql_type)

            if i < len(groupby_block) - 1:
                sql_command += ', '

    # construct order by block
    if len(sql_tree['ordering']) > 0:
        sql_command += ' order by '

        orderby_block = sql_tree['ordering']

        for i, ordering in enumerate(orderby_block):
            if ordering['table_or_alias_name'] is not None:
                sql_command += ordering['table_or_alias_name']
                sql_command += '.'
            # replace non-standard SQL parts with Postgres SQL parts
            sql_command += \
                syntax_replace_posgres(ordering['column_name'],
                                       sql_type)

            if i < len(orderby_block) - 1:
                sql_command += ', '

    # construct having block
    if len(sql_tree['having']) > 0:
        sql_command += ' having '

        having_block = sql_tree['having']

        for i, having in enumerate(having_block):
            # replace non-standard SQL parts with Postgres SQL parts
            sql_command += syntax_replace_posgres(having, sql_type)

            if i < len(having_block) - 1:
                sql_command += ', '

    # finish query
    sql_command += ';'

    return sql_command


if __name__ == '__main__':

    import sql_to_tree

    input_sql = """select c.customer_name, order_date, sum(o.orders), sysdate
                from tcph.customer as c, tcph.order o, tcph.part
                where c.customer_id = o.customer_id
                and o.part_number = tcph.part.part_number
                and c.customer_id = 1
                group by c.customer_name, o.order_date
                order by c.customer_name;"""

    print(tree_to_postgres_sql(sql_to_tree.sql_to_tree(input_sql), 'Oracle'))
