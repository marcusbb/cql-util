package driver.em;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import driver.em.CUtils;
import driver.em.CassConfig;

public abstract class TestBase {

	protected static Session session;
	protected static Cluster cluster;
	protected static String ks = "olympia";
	
	protected static String [] cHosts = {"localhost"};
	
	
	protected static CassandraCQLUnit cassandraCQLUnit = null;
	
	@BeforeClass
	public static void beforeClass() throws ConfigurationException, TTransportException, IOException, InterruptedException {
		try {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-unit.yaml");
		}catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		CassConfig config = new CassConfig();
		config.setContactHostsName(cHosts);
		config.setNativePort(9142);
		Cluster cluster = CUtils.createCluster(config);
		//cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("olympia.cql",true, ks));
		CQLDataLoader loader = new CQLDataLoader(cluster.connect());
		loader.load(new ClassPathCQLDataSet("olympia.cql",true, ks));
		session = loader.getSession();
		//session = CUtils.createSession(cluster, ks);
		
		
	}
	protected static void createKS(Cluster cluster) {
		Session sysSession = cluster.connect("system");
		
		try {
			
			session.execute("use olympia");
		}catch (Exception e) {
			session.execute("create keyspace olympia");
		}
		
	}
	
	@AfterClass
	public static void afterClass() {
		//TODO clean up data and tear down
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}
	
	public static byte[] serialize(Object obj) throws IOException {
		byte []b = null;
		ObjectOutputStream out = null;
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			out = new ObjectOutputStream(bout);
			out.writeObject(obj);
			b = bout.toByteArray();
		} finally {
			if (out != null) {
				out.close();
			}
		}
		return b;
	}
	
	public static Object deserialize(ByteBuffer bb) throws IOException,ClassNotFoundException {
		byte []b = new byte[bb.remaining()];
		int i=0;
		while (bb.remaining() >0)
			b[i++] = bb.get();
		
		return deserialize(b);
		
	}
	public static  Object deserialize(byte []b) throws IOException,ClassNotFoundException {
		Object obj = null;
		ObjectInputStream oin = null;
		try {
			//System.out.println("read obj: " + b.length);
			ByteArrayInputStream bin = new ByteArrayInputStream(b);
			oin = new ObjectInputStream(bin);
			obj = oin.readObject();
			return obj;
		} finally {
			if (oin != null) {
				oin.close();
			}
		}
	}
	

}
