Motivation:
Do batch processing on top of the C* cluster without having to do an integration of hadoop a top of C* nodes.
Management of a name node, job tracker and other components in the hadoop ecosystem is complicated.
MR is typically hard to program and manage without helper technologies like Hive or Pig.


This package contains a distributed CQL Row Reader integrating Hazelcast as a distributed execution framework for
reading rows from Cassandra.

The split of tokens is simple currently implying an even split between of row tokens among all the participating members
of the row reading.


Still thinking about a progressive call back functionality - which would be either.
 