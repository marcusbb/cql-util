CQL Utilities

Utilities built for Cassandra Server http://cassandra.apache.org, on top of the java driver: https://github.com/datastax/java-driver

The project originally as entity mapping on the java driver 2.0, but has morphed into a set of other useful stuff, that depends on the driver, but can be built on top of any CQL compliant client.

Currently this is broken into 3 distinct projects

1. [Entity mapping for the java driver ]
(tree/master/src/main/java/driver/em/README.txt)

2. [JDBC to Cassandra migration tool]
(tree/master/src/main/java/migration/README.txt)

3. [All rows reader]
(tree/master/src/main/java/reader/README.txt)
