#!/usr/bin/python
import extract_table_names
import extract_selected_columns
import extract_where
import extract_orderby
import extract_aggregates
import extract_having


def sql_to_tree(input_sql):
    '''Converts SQL to a SQL tree.  Does not validate that input SQL is correct.

    keyword args:
        input_sql: SQL text input.
            May be multiple lines, so long as they are separated by semicolon


    returns:
        list of sql tree components
    '''

    sql_tree = {}

    # todo: refactor all of these so that they take parsed
    #                           SQL stream rather than raw SQL

    sql_tree['select'] = \
        extract_selected_columns.extract_select(input_sql)
    sql_tree['select aggregate'] = \
        extract_selected_columns.extract_select_aggregates(input_sql)
    sql_tree['table_definitions'] = \
        extract_table_names.extract_table_definitions(input_sql)
    # needs support for outer joins
    sql_tree['joins'] = extract_where.extract_joins(input_sql)
    sql_tree['filters'] = extract_where.extract_filters(input_sql)
    # need to split out join from subquery
    sql_tree['where_subqueries'] = \
        extract_where.extract_where_subqueries(input_sql)
    sql_tree['grouping'] = extract_aggregates.extract_groupby(input_sql)
    sql_tree['ordering'] = extract_orderby.extract_orderby(input_sql)
    sql_tree['having'] = extract_having.extract_having(input_sql)

    return sql_tree


if __name__ == '__main__':

    input_sql = """
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= '1998-12-01'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;"""

    for k, v in sql_to_tree(input_sql).items():
        print k, v
