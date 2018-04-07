select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice),
	sum(l_extendedprice),
	sum(l_extendedprice),
	avg(l_quantity),
	avg(l_extendedprice),
	avg(l_discount),
	count(*)
from
	tcph.lineitem
where
	l_shipdate <= '1998-12-01'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
