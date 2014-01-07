CQL Utilities

Utilities built for Cassandra Server http://cassandra.apache.org, on top of the java driver: https://github.com/datastax/java-driver

The project originally as entity mapping on the java driver 2.0, but has morphed into a set of other useful stuff, that depends on the driver, but can be built on top of any CQL compliant client.

Currently this is broken into 3 distinct projects

1. [Entity mapping for the java driver ]
(master/src/main/java/driver/em/)

2. [JDBC to Cassandra migration tool]
(master/src/main/java/migration/)

3. [All rows reader]
(master/src/main/java/reader/)
