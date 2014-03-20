mvn clean install -Ptestmigration
$JAVA_HOME/bin/java -Dlog4j.configuration=file:`pwd`/src/test/java/migration/log4j.xml -Xms1024m -Xmx1024m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -classpath target/cqlutil-migration-it-test.jar migration.Main src/test/java/migration/async-mapping.xml
