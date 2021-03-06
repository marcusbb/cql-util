And all rows reader.  Or a reader with configurable columns returned and read.

The algorithm is premised with the CQL support for token query statements.
select col1,col2 where token(part_key) > starttoken and token(part_key) <= endtoken.
*part_key => Cassandra partitioning key defined by the partitioning strategy of the server.

It takes care of pagination (ie. limit) and discarding duplicates.
Taking care of duplicates and handling of the token ranges are the real value of this.

There are basic recipes for doing basic aggregate functions - this has been moved to the 
samples package.
LoggingRowTask - logs and nothing more
RowCountTask - total CQL row count

 
Why not use count(*)?  If you have even a somewhat reasonable number of rows (millions)
than you're almost sure to have a OOM doing this aggregation operation on the server.
This tool helps you process all rows in a more defined manner that won't kill C* server
and won't timeout in the process.

Features: configurable start and end token, so that you may split and distribute the 
process or tasks separately.  This will lend itself to a multi-threaded fetch or a
distributed fetch operation.

Currently supported is a multi-threaded fetch, spliting the token ranges equally by the number
of executing threads.

Why do we need to know the composite row key?  The algorithm does not assume that each
partition row key is a primary key, as it could be parts.  And further more, we
want to exclude rows that read again.  As the algorithm doesn't know first hand the 
number of CQL rows returned per partition key.


There is a current limitation of the cql token function that prevents more than one argument.
TODO: figure out how the composite row key token is generated

For the prototypical example see RowCountJob or DistinctCountJob

java -cp $CP reader.CQLRowReaderImproved -Dconfig=reader-config.xml

Configuration is defined via xml, but can be hand stitched.


<config>
	<cassConfig>
		...
		<contactHostsName>localhost</contactHostsName>
		...
	</cassConfig>
	<keyspace>icrs</keyspace>
	<table>devices</table>
	<pageSize>1000</pageSize>
	<!--beginning of range -->
	<startToken></startToken>
	<!-- end of range -->
	<endToken></endToken>
	<!-- Not recommended to manually configure, as this will be discovered -->
	<pkConfig>
	  	<!-- This is the partition portion of the key: one or more -->
		<partitionKeys>
			<name>id</name>
			<type>ASCII</type>
		</partitionKeys>
		<partitionKeys>
			<name>id2</name>
			<type>ASCII</type>
		</partitionKeys>
		<!-- One or more non-partitioned parts -->
		<clusterKeys>
			<name>name</name>
			<type>ASCII</type>
		</clusterKeys>
		
	</pkConfig>
</config>