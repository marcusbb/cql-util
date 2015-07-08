package migration;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import driver.em.CUtils;
import driver.em.CassConfig;

public class MigrationBaseTest {

	protected static String jdbcDriver = "org.apache.derby.jdbc.EmbeddedDriver";
	protected static String jdbcUrl = "jdbc:derby:memory:testDB;create=true";
	
	protected static Cluster cluster;
		
	protected static String[] keyspaces = {"migration_ks","migration_ks2"};
	
	@BeforeClass
	public static void beforeClass() {

		try {
			//cassandra
			EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-unit.yaml");
		
			//CQL setup
			CassConfig config = new CassConfig();
			config.setContactHostsName(new String[]{"localhost"});
			config.setNativePort(9142);
			cluster = CUtils.createCluster(config);
			CQLDataLoader loader = new CQLDataLoader(cluster.connect());
			for (String ks:keyspaces)
				loader.load(new ClassPathCQLDataSet("migration/setup.cql",true, ks));
			
			//start derby
			Connection con = getDerbyCon(true);
			if (!tableExists(con, "devices"))
				executeResource(con, "migration/derby.sql");
			con.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	protected static Session getSession(String keyspace) {
		return cluster.connect(keyspace);
	}
	protected static Connection getDerbyCon() throws ClassNotFoundException, SQLException {
		return getDerbyCon(false);
	}
	private static Connection getDerbyCon(boolean create) throws ClassNotFoundException, SQLException {
		Class.forName(jdbcDriver);
		Connection con = DriverManager
				.getConnection("jdbc:derby:memory:testDB;create=" + create);
		return con;
	}
	private static void executeResource(Connection con, String resource) throws IOException,SQLException {
		InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        byte []b = new byte[ins.available()];
        ins.read(b);
        con.createStatement().execute(new String(b));
	}
	private static boolean tableExists(Connection con, String table) throws IOException,SQLException {
		try {
			con.createStatement().execute("select 1 from " + table);
			return true;
		}catch (SQLException e) {
			return false;
		}
	}
}
