# Request For Comments

Currently the CQL Utils provide a blunt force instrument to copy data from an Oracle/MySQL or any other RDBMS source with a valid jdbc driver and pushing that data into Cassandra.
Here is a non-exhaustive list of things that the tools should provide:

- Fast
- Configurable SQL Result query
- Push to one or many Cassandra column families/keyspaces
- Monitoring via JMX - how many jdbc records processed, how many cassandra rows inserted/updated
- Manage-able - ability to pause the job for an arbitrary amount of time
- Tunable Concurrency options
- Ability to identify delta - update Cassandra rows that more stale than relational store
- Split-able - Ability to split the job into multiple for high scale throughput


# Motivation
Move data from Relation databases (MySQL and Oracle) to Cassandra using the CQL driver.

Why not build custom?  Ie. implement the entities provided by em support?
Because the data will likely not be migrated in a one to one manner.

Supported is running native queries against Oracle or MySQL and inserting into one or more Cassandra table structures.
Relying on the underlying cursor state of the jdbc driver.

Splitting of jobs into reasonable sizes is not possible currently since SQL is ad-hoc and can be joined in ways that make it inherently unsplit-able.
Its recommended that each large data-set is examined for indexed columns that can be used at a mechanism for partitioning the relational data.

Transformation and filtering are currently supported.
This is a raw data to data.  Most databases will provide some sufficient transformative capabilities in querying the data such as string and date transform functions so 
even though the data types don't map directly from jdbc to CQL, you will be able to likely cast it from the query.


The full set of configuration can be obtained from source [XMLConfig] (XMLConfig.java)
Sample XML:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<mapping>
	<jdbcUrl><![CDATA[jdbc:mysql://localhost/hera?user=root&password=]]></jdbcUrl>
	
	<jdbcDriver>oracle.jdbc.driver.OracleDriver</jdbcDriver>
	<jdbcUsername>username</jdbcUsername>
	<jdbcPassword>password</jdbcPassword>
	
	<asyncWrites>true</asyncWrites>
	
	<sqlQuery><![CDATA[
	SELECT  u.userName, f.*,0 as version, p.* FROM fileTbl f, filePropTbl p,userTbl u, clientTbl c where 
	f.fileId_pk = p.fileId and c.clientId_pk = f.clientId and u.userId_pk = c.userId 
	]]></sqlQuery>

	<forEach>
		<rsToCqlConfig>
		    <keyspace>ks1</keyspace>
			<cqlTable>users</cqlTable>
			<columns>
				<column>
					<jdbcName>userName</jdbcName>
					<cqlName>username</cqlName>
					<isPK>true</isPK>
					<type>TEXT</type>
				</column>
				<column>
					<jdbcName>fileNameForSearch</jdbcName>
					<cqlName>filename</cqlName>
					<isPK>true</isPK>
					<type>TEXT</type>
				</column>
				<column>
					<jdbcName>version</jdbcName>
					<cqlName>version</cqlName>
					<isPK>true</isPK>
					<type>BIGINT</type>
				</column>
				<column>
					<jdbcName>md5Hash</jdbcName>
					<cqlName>md5</cqlName>
					<isPK>false</isPK>
					<type>TEXT</type>
				</column>
				
				<column>
					<jdbcName>dateOfCreation</jdbcName>
					<cqlName>dateofcreation</cqlName>
					<isPK>false</isPK>
					<type>BIGINT</type>
				</column>
			</columns>
		</rsToCqlConfig>
		<rsToCqlConfig>
			<keyspace>ks1</keyspace>
			<cqlTable>users</cqlTable>
			<columns>
				<column>
					<jdbcName>userName</jdbcName>
					<cqlName>username</cqlName>
					<isPK>true</isPK>
					<type>TEXT</type>
				</column>
				<column>
					<jdbcName>fileNameForSearch</jdbcName>
					<cqlName>filename</cqlName>
					<isPK>true</isPK>
					<type>TEXT</type>
				</column>
				<column>
					<jdbcName>version</jdbcName>
					<cqlName>version</cqlName>
					<isPK>true</isPK>
					<type>BIGINT</type>
				</column>
			</columns>
			<nameMapping>
				<jdbcName>type</jdbcName>
				<cqlName>properties</cqlName>
				<isPK>false</isPK>
				<type>TEXT</type>
			</nameMapping>
			<valueMapping>
				<jdbcName>propValue</jdbcName>
				<cqlName>properties</cqlName>
				<isPK>false</isPK>
				<type>TEXT</type>
			</valueMapping>
		</rsToCqlConfig>
	</forEach>
	
</mapping>
```
