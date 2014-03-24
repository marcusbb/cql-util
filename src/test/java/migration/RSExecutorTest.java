package migration;

import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Test;

public class RSExecutorTest {

	@Ignore
	@Test
	public void logFailedCql() throws ClassNotFoundException, SQLException{
		String queryString="BEGIN UNLOGGED BATCH INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-24T10:00:00-0400', 'AB774100' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774199' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-31T17:00:00-0400', 'PINA' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774101' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-31T17:00:00-0400', 'PIN2' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-24T10:00:00-0400', 'AB774102' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-24T10:00:00-0400', 'AB774148' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774149' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-24T10:00:00-0400', 'AB774150' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774123' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-24T10:00:00-0400', 'AB774124' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774125' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774173' ) ; INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2014-03-24T10:00:00-0400', 'AB774174' ) ;INSERT INTO last_auth_activity ( auth_ts, device_id )  VALUES ( '2013-12-18T08:00:00-0500', 'AB774175' ) ;APPLY BATCH;";
		String keyspace = "my_keyspace";
		XMLConfig config = new XMLConfig();
		config.setKeyspace(keyspace);
		config.setBatchWrites(15);
		RSExecutor executor = new RSExecutor(config);
		executor.logFailedCql(keyspace, queryString);
		//logFailedCql
	}
	
}
