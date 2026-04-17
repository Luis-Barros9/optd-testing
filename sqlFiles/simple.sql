select orders.o_orderkey, orders.o_orderdate,orders.o_custkey, lineitem.l_partkey from customer
join orders on customer.c_custkey = orders.o_custkey
join lineitem on orders.o_orderkey = lineitem.l_orderkey
where c_mktsegment = 'BUILDING' and orders.o_orderkey > 1500;