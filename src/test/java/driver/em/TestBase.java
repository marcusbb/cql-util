package driver.em;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


import driver.em.CUtils;
import driver.em.CassConfig;

public abstract class TestBase {

	protected static Session session;
	protected static Cluster cluster;
	protected static String ks = "olympia";
	
	protected static String [] cHosts = {"localhost"};
	@BeforeClass
	public static void beforeClass() {
		CassConfig config = new CassConfig();
		config.setContactHostsName(cHosts);
		Cluster cluster = CUtils.createCluster(config);
		session = CUtils.createSession(cluster, ks);
		
	}
	@AfterClass
	public static void afterClass() {
		//TODO clean up data and tear down
	}
	
	

}
