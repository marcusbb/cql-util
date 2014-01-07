And all rows reader.  Or a reader with configurable columns returned and read.

The algorithm is premised with the CQL support for token query statements.
select col1,col2 where token(part_key) > starttoken and token <= endtoken.

It takes care of pagination (ie. limit) and discarding duplicates.
Taking care of duplicates and handling of the token ranges are the real value of this.

There are basic recipes for doing basic aggregate functions:
LoggingRowTask - logs and nothing more
RowCountTask - total CQL row count

 
Why not use count(*).  If you have even a somewhat reasonable number of rows (millions)
than you're almost sure to have a OOM doing this aggregation operation on the server.
This tool helps you process all rows in a more defined manner that won't kill C* server
and won't timeout in the process.

Features: configurable start and end token, so that you may split and distribute the 
process or tasks separately.  This will lend itself to a multi-threaded fetch or a
distributed fetch operation.

Configuration based knowlege of table, partition column.  

Why do we need to know the composite row key?  The algorithm does not assume that each
partition row key is a primary key, as it could be parts.  And further more, we
want to exclude rows that read again.  As the algorithm doesn't know first had the 
number of CQL rows returned per partition key.


There is a current limitation of the cql token function that prevents more than one argument.
TODO: figure out how the composite row key token is generated

For the prototypical example see RowCountTask.

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
	
	<pkConfig>
	  	<!-- This is the partition portion of the key -->
		<tokenPart>
			<name>id</name>
			<type>ASCII</type>
		</tokenPart>
		<!-- One or more non-partitioned parts -->
		<nonTokenPart>
			<name>name</name>
			<type>ASCII</type>
		</nonTokenPart>
		
	</pkConfig>
</config>