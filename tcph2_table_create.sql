drop table tcph.partsuppcost;

create table tcph.partsuppcost
as
select
			ps_partkey as psc_partkey,
			min(ps_supplycost) as psc_min_supplycost
		from
			tcph.partsupp,
			tcph.supplier,
			tcph.nation,
			tcph.region
		where
			s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'AFRICA'
		group by ps_partkey