# CQL Utilities

Utilities built for Cassandra Server http://cassandra.apache.org, on top of the java driver: https://github.com/datastax/java-driver

The project originally as entity mapping on the java driver 2.0, but has morphed into a set of other useful stuff, that depends on the driver, but can be built on top of any CQL compliant client.

### Building
I haven't come to terms with guava exclusion with respect to running cassandra unit tests.
Therefore building and testing are 2 seperate events:
Compilation (and installation)
```bash
mvn install 
```
Testing
```bash
mvn test -f pom_guava.xml
```
**If anyone can point me to the correct solution I'd be happy to buy you a [virtual beer](http://beeroverip.org/)**

### Features
Currently this is broken into 3 distinct projects

* [Entity mapping for the java driver]
(master/src/main/java/driver/em/)
Main motivation is to reduce boilerplate code to map data structure classes (beans) with CQL.
You are likely to be able to use the DataStax mapping features out of the box, although this comes with some handy features like prepared statement caching and batch statement support.  As well as mapping to Maps, as this was seen as a good candidate to provide some EM.

Soon to add: Enabled participation in an XA (JTA) transaction.

* [JDBC to Cassandra migration tool]
(master/src/main/java/migration/)
A configuration driven mechanism for moving data from MySQL/Oracle (or any jdbc interface) to Cassandra.

As with the all rows reader (below), you are likely to want to leverage Spark for this type of activity, although this provides a completely descriptive approach (XML).


* [All rows reader]
(master/src/main/java/reader/)
Paginated loading of all rows, with configurable tokens, and columns to query.
3a. [Distributed all rows reader]
(master/src/main/java/reader/dist)
which currently is hosted in this project but can be moved out.  Clients will have a hazelcast 3.x (to be removed) dependency but need not use it as the CQLReader works in stand alone mode as well.

In all likelyhood if you require some batch processing, you'll use Spark with the Cassandra connector.  
This is not meant to be a replacement or even coming close to matching the feature set but does allow you to run batch operations against Cassandra from a stand-alone application without the need for a spark context/cluster.

Requirements:
- Java 1.7
- Maven - not published to any public repos as of yet
- Cassandra - a cluster to connect to, both 1.2.x and 2.0.x have been tested. 


Want to participate or have questions?  Please email me at msimonsen@gmail.com

