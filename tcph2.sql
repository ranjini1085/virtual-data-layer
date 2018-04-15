select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	tcph.part,
	tcph.supplier,
	tcph.partsupp,
	tcph.partsuppcost,
	tcph.nation,
	tcph.region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 1
	and p_type = 'MEDIUM BRUSHED BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
  and r_name = 'AFRICA                   '
	and ps_supplycost = psc_min_supplycost
order by
	s_acctbal,
	n_name,
	s_name,
	p_partkey;
