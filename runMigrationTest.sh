mvn clean install -Ptestmigration
$JAVA_HOME/bin/java  -Xms1024m -Xmx1024m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -classpath target/cqlutil-migration-it-test.jar migration.Main src/test/java/migration/async-mapping.xml
