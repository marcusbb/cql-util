CQL Utilities

Utilities built for Cassandra Server http://cassandra.apache.org, on top of the java driver: https://github.com/datastax/java-driver

The project originally as entity mapping on the java driver 2.0, but has morphed into a set of other useful stuff, that depends on the driver, but can be built on top of any CQL compliant client.

Currently this is broken into 3 distinct projects

1. [Entity mapping for the java driver]
(master/src/main/java/driver/em/)
Main motivation is to reduce boilerplate code to map data structure classes (beans) with CQL.

2. [JDBC to Cassandra migration tool]
(master/src/main/java/migration/)
A completely configuration driven mechanism for moving data from MySQL/Oracle to Cassandra.

3. [All rows reader]
(master/src/main/java/reader/)
Paginated loading of all rows, with configurable tokens, and columns to query.

Currently all unit tests require a server to be running on a local machine with keyspace icrs provisioned.
I'll change that shortly.

Want to participate or have questions?  Please email me at msimonsen@blackberry.com

