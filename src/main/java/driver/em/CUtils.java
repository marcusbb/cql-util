package driver.em;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


import com.datastax.driver.core.BatchStatement;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import driver.em.CassConfig.LoadBalancing;


/**
 * 
 * Convenience utils for driver and cassandra
 *
 */
public class CUtils {

	public enum Name {

        ASCII     (DataType.ascii()),
        BIGINT    (DataType.bigint()),
        BLOB      (DataType.blob()),
        BOOLEAN   (DataType.cboolean()),
        COUNTER   (DataType.counter()),
        DECIMAL   (DataType.decimal()),
        DOUBLE    (DataType.cdouble()),
        FLOAT     (DataType.cdouble()),
        INET      (DataType.inet()),
        INT       (DataType.cint()),
        TEXT      (DataType.text()),
        TIMESTAMP (DataType.timestamp()),
        UUID      (DataType.uuid()),
        VARCHAR   (DataType.varchar()),
        VARINT    (DataType.bigint()),
        TIMEUUID  (DataType.timeuuid()),
        //LIST      (DataType.list(elementType)),
        //SET       (34, Set.class),
        MAP       (DataType.map(DataType.text(), DataType.text()));
        //CUSTOM    (0,  ByteBuffer.class);
        private DataType type;
        private Name(DataType type) {
        	this.type = type;
        }
        public DataType getType() {
        	return type;
        }
        public static DataType parseType(String type) {
        	return Name.valueOf(type).getType();
        }
	}
    
	//Might make this a Map<ReqConstants,Object>
	//TODO: make this an immutable map
	private static Map<String,Object> defParms = new HashMap<>(); 
	
	static {
		defParms.put(ReqConstants.CONSISTENCY.toString(), ConsistencyLevel.LOCAL_QUORUM);
		defParms.put(ReqConstants.RETRY_POLICY.toString(), RetryPolicy.RetryDecision.rethrow());
	}
	
	public static Session createSession(Cluster cluster, String keyspace){
		
		Session session = cluster.connect(keyspace);
		return session;
	}
	
	public static Map<String,Object> getDefaultParams() {
		return defParms;
	}
	public static Map<String,Object> getDefParamsWithConsistency(ConsistencyLevel level) {
		HashMap<String, Object> map = new HashMap<>(defParms);
		map.put(ReqConstants.CONSISTENCY.toString(), level);
		return map;
	}
	public static Map<String,Object> getDefParamsWithConsistency(ConsistencyLevel level,BatchStatement batch) {
		HashMap<String, Object> map = new HashMap<>(defParms);
		map.put(ReqConstants.CONSISTENCY.toString(), level);
		map.put(ReqConstants.BATCH.toString(), batch);
		return map;
	}
	
	public static Cluster createCluster(CassConfig context){
		//pooling options
		
		
        PoolingOptions pools = new PoolingOptions();
        pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, context.getConcurrentLocal());
        pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, context.getConcurrentRemote());
        
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, context.getCoreConnectionsPerLocalHost());
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, context.getMaxConnectionsPerLocalHost());
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, context.getCoreConnectionsPerRemoteHost());
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, context.getMaxConnectionsPerRemoteHost());
        
        //socket options
        SocketOptions sockOpts = new SocketOptions();
        sockOpts.setTcpNoDelay(context.isTcpNoDelay());
        //TODO: need to figure out balancing policy and retry policy from config
        LoadBalancingPolicy lbPolicy = null;
        if (context.loadBalancing == LoadBalancing.TOKEN_AWARE_DC_RR)
        	lbPolicy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy(context.getLocalDataCenterName()));
        else if (context.loadBalancing == LoadBalancing.ROUND_ROBIN)
        	lbPolicy = new RoundRobinPolicy();
        else if (context.loadBalancing == LoadBalancing.TOKEN_AWARE_DC_RR)
        	lbPolicy = new TokenAwarePolicy(new RoundRobinPolicy());
        
        Builder builder = Cluster.builder()
				  .addContactPoints(context.getContactHostsName())
				  .withPort(context.getNativePort())
				  .withLoadBalancingPolicy(lbPolicy)
				  .withReconnectionPolicy(new ExponentialReconnectionPolicy(context.getBaseReconnectDelay(), context.getMaxReconnectDelay()))
				  .withPoolingOptions(pools)
				  .withSocketOptions(sockOpts)
				  //The default is sufficient
				  //and can be handled on a per request basis
				  .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));
      
        if(context.getUsername() != null){
        	builder.withCredentials(context.getUsername(), context.getPassword());
		}
      
		return builder.build();		
	}
	
	public static ByteBuffer[] getBytesForRoute(Object ...objs ) {
		ByteBuffer []buffers = new ByteBuffer[objs.length];
		int i=0;
		for (Object value:objs) {
			buffers[i++] = TypeCodec.getDataTypeFor(value).serialize(value);
		}
		
		
		return buffers;
	}
	@Deprecated
	public static ByteBuffer[] getBytesForRoute(String ...strings ) {
		ByteBuffer []buffers = new ByteBuffer[strings.length];
		int i=0;
		for (String s:strings) {
			buffers[i++] = ByteBuffer.wrap(s.getBytes());
		}
		return buffers;
	}
}
