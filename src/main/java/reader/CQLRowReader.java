package reader;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CQLRowReader {

	private Cluster cluster;
	
	private Session session;
	
	
	static Properties properties = null;
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		properties = new Properties();
		
		properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties"));
		
		CQLRowReader reader = new CQLRowReader();
		
				
		reader.cluster = reader.createCluster();
		reader.session = reader.cluster.connect("icrs");
		boolean more = true;
		String startToken = "0";
		Long token = Long.MIN_VALUE;
		int pageSize = 10;
		long count = 0;
		while (more) {
			
			//SimpleStatement ss = new SimpleStatement("select id,token(id) from devices where token(id) > token('" + startToken + "') limit " +pageSize);
			SimpleStatement ss = new SimpleStatement("select id,token(id) from devices where token(id) > " + token + " limit " +pageSize);
			ResultSet rs = reader.session.execute(ss);


			Iterator<Row> iter = rs.iterator();
			if (!iter.hasNext())
				more = false;
			while (iter.hasNext()) {
				count++;
				Row row = iter.next();
				startToken = row.getString(0);
				token = row.getLong(1);
			}
			System.out.println("Count: " + count + " start: " + startToken + " token: " + token);
		}
		
		
		System.exit(0);
	}

	private String getProperty(String key) {
		return properties.getProperty(key);
	}
	private Integer getIntProperty(String key) {
		return Integer.parseInt(properties.getProperty(key) );
	}
	private Cluster createCluster(){
		Cluster cluster = Cluster.builder()
				  .addContactPoints(getProperty("driver.contacts"))
				  .withPort(getIntProperty("driver.port"))
				  .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(getProperty("driver.dcname"))))
				  .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000,60000))
				  .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
				  .withCredentials(getProperty("driver.username"), getProperty("driver.password"))
				  
				  .build();
		// configure connection pool
		cluster.getConfiguration().getPoolingOptions()
				
		       .setMaxConnectionsPerHost(HostDistance.LOCAL, getIntProperty("driver.maxconlocal"))
		       .setMaxConnectionsPerHost(HostDistance.REMOTE, getIntProperty("driver.maxconremote"))
		       .setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, getIntProperty("driver.simreq_conlocal"))
		       .setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, getIntProperty("driver.simreq_conremote"));
		// configure connection options
		cluster.getConfiguration().getSocketOptions()
		       //.setConnectTimeoutMillis(Integer.MAX_VALUE)
		      // .setKeepAlive(true)
		      // .setSoLinger(1000)
		       .setConnectTimeoutMillis(Integer.MAX_VALUE);
		       
		
		return cluster;		
	}
	
}
