select
	l_orderkey,
	sum(l_extendedprice),
	o_orderdate,
	o_shippriority
from
	tcph.customer,
	tcph.orders,
	tcph.lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1997-12-31'
	and l_shipdate > '1998-01-01'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	sum(l_extendedprice) desc,
	o_orderdate;

