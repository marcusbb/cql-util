
Motivation:
Move data from Relation databases (MySQL and Oracle) to Cassandra using the CQL driver.

Why not build custom?  Ie. implement the entities provided by em support?
Because the data will likely not be migrated in a one to one manner.

Supported is running native queries against Oracle or MySQL and inserting into one or more Cassandra table structures.
Relying on the underlying cursor state of the jdbc driver.

Why not provide pagination? See above, but also because the split will be defined in the native SQL, and partitioned
according to the migration process.
It's designed to be run in parallel where multiple migration processes are run to complete the 
process.

Why aren't transformations supported?  This is a raw data to data.  Most databases will provide
some sufficient transformative capabilities in querying the data such as string and date transform functions so 
even though the data types don't map directly from jdbc to CQL, you will be able to likely cast it from the query.
 
SQL is powerful - a custom transformation is hard to implement in a dynamic way... although certainly
still possible.

Only a sinlge threaded fetch model is supported, although a customized executor service can be implemented 
for operations against the driver - although writes to C* should be very fast.
Possible mechanism for paged or bulk loading is also possible.

The full set of configuration can be obtained from source [XMLConfig] (master/src/main/java/XMLConfig.java)
Sample XML:
```
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