
Custom Resolvers
----------------

This fork implements a version of custom resolvers, described loosely in CASSANDRA-6412

Example
-------

Financial markets often track 4 fields for each stock each day:
- Opening price
- Closing price
- High price
- Low price

The traditional way to model this would be to either:
- Track all prices, and then either use UDF to find min/max at runtime, or 
- Read before write, and manually check to see if the incoming write is min/max

Using custom resolvers, this becomes trivial:
 
```
ccm node1 cqlsh
Connected to test at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.0-rc1-SNAPSHOT | CQL spec 3.3.1 | Native protocol v4]
Use HELP for help.
cqlsh> create keyspace test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; use test; 
cqlsh:test> create table test ( 
		id text primary key, 
		last int, 
		first int with resolver 'org.apache.cassandra.db.resolvers.ReverseTimestampResolver', 
		high int with resolver 'org.apache.cassandra.db.resolvers.MaxValueResolver', 
		low int with resolver 'org.apache.cassandra.db.resolvers.MinValueResolver');
cqlsh:test> insert into test(id,last,first,high,low) values ('a', 1, 1, 1, 1); 
cqlsh:test> insert into test(id,last,first,high,low) values ('a', 2, 2, 2, 2);
cqlsh:test> insert into test(id,last,first,high,low) values ('a', 3, 3, 3, 3);
cqlsh:test> insert into test(id,last,first,high,low) values ('a', 5, 5, 5, 5);
cqlsh:test> insert into test(id,last,first,high,low) values ('a', 4, 4, 4, 4);
cqlsh:test> select * from test where id='a';

 id | first | high | last | low
----+-------+------+------+-----
  a |     1 |    5 |    4 |   1

(1 rows)
```

Concept
-------

Rather than relying only on last-write-win, Cassandra can use more intelligent 
conflict resolution when reconciling multiple cells. This not only allows faster 
and more efficient concepts like 'max()' or 'min()', but also enables first-write-wins
and could potentially be used to enable far more complex resolution.

Each non-PK column can be defined with a CellResolver, which can implement 
resolveRegular() for reconciling two standard cells, and supportsType()
to validate which type(s) the resolver supports.


Current Limitations
-------------------

- At the current time, a resolver can be applied to a UDT within a table, but not cells
within a UDT

- At the current time, a resolver can not be specified on a column in an MV, though 
a resolver can be set on a MVs base table

Tombstones
----------

The primary limitation at this point appears to be handling of tombstones, notably 
situations such as

Resolver: Numerical Max

- First write: 2

- Second write: delete (tombstone)

- Third write: 1

In this case, one would expect that the numerical max would be 1, however, depending on
the state of tombstone GC, it is possible that cells may be reconciled in an order where
the third write takes precedence over the second, and then the second write (tombstone)
takes precedence over the first, leaving a null value where one would rightly expect 1.


