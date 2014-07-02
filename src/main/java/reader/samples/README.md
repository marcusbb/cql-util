
Samples contained within this package provides some guidance on how to build your reader job.

The reader job is broken down into 2 main pieces.

1. The bootstrap - This contains the configuration, C* cluster and other initialization required.
There are 2 flavours of bootstrap class, a single threaded and multi-threaded variety.
The bootstrap functionality also mandates an implementation "initJob" method which requires the
developer to provide a ReaderJob itself. This class is to be defined by the developer.

2. The ReaderJob - This class must provide the necessary row processing, and must implement a method
to return a "task" that will process Cassandra row.  The task class itself must implement the RowReaderTask.
The RowReaderTask will be invoked by the "initJob" from the bootstrap, and called as many times as there
are rows to be read. 