package reader;

import java.util.HashSet;
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

public class CQLRowReaderImproved {

	private Cluster cluster;
	
	private Session session;
	
	
	static Properties properties = null;
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		properties = new Properties();
		
		properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties"));
		
		CQLRowReaderImproved reader = new CQLRowReaderImproved();
		
				
		reader.cluster = reader.createCluster();
		reader.session = reader.cluster.connect("icrs");
		boolean more = true;
		String startId = "0";
		Long token = Long.MIN_VALUE;
		int pageSize = 13;
		long count = 0;
		HashSet<String> lastIdSet = new HashSet<String>(pageSize);
		
		while (more) {
			
			//SimpleStatement ss = new SimpleStatement("select id,token(id) from devices where token(id) > token('" + startToken + "') limit " +pageSize);
			System.out.println("StartToken: " + token + " startId " +startId);
			SimpleStatement ss = new SimpleStatement("select id,name,token(id) from devices where token(id) >= " + token + " limit " +pageSize);
			ResultSet rs = reader.session.execute(ss);


			Iterator<Row> iter = rs.iterator();
			
			//bail on empty result set
			if (!iter.hasNext())
				break;
			
			String lastId = null;
			
			Row row = null;
			int curRowCount = 0;
			HashSet<String> curIdSet = new HashSet<String>(pageSize);
			
			while (iter.hasNext()) {
				curRowCount++;
				row = iter.next();
				lastId = row.getString(0)+":"+row.getString(1);
				//System.out.println("lastId: " + lastId);
				curIdSet.add(lastId);
				if (lastIdSet.contains(lastId)) {
					continue;
				}
				
				token = row.getLong(2);
				
				count++;
			}
			lastIdSet = curIdSet;
			//exhausted the rs
			if (curRowCount < pageSize ) {
				System.out.println("page size " + pageSize+ " exceeds row count " + curRowCount);
				//System.out.println("Count: " + count + " start: " + startId + " token: " + token);
				token++;
				//break;
			}
			startId = lastId;
			//didn't read any new id's, move on to next token
			
			
			System.out.println("Count: " + count + " start: " + startId + " token: " + token);
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
		       .setMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.LOCAL, getIntProperty("driver.simreq_conlocal"))
		       .setMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.REMOTE, getIntProperty("driver.simreq_conremote"));
		// configure connection options
		cluster.getConfiguration().getSocketOptions()
		       //.setConnectTimeoutMillis(Integer.MAX_VALUE)
		      // .setKeepAlive(true)
		      // .setSoLinger(1000)
		       .setConnectTimeoutMillis(Integer.MAX_VALUE);
		       
		
		return cluster;		
	}
	
}
