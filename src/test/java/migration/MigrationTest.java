package migration;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import util.JAXBUtil;

public class MigrationTest extends MigrationBaseTest {

	//@Before
	public void prep(String prefix,int dev_count) throws Exception {
		//populate devices table
		Connection con = getDerbyCon();
		
		for (int i=0;i<dev_count;i++) {
			PreparedStatement ps = con.prepareStatement("insert into devices (pin,os,os_ver,ts) values (?,?,?,?)");
			int pi = 1;
			ps.setString(pi++, prefix + "_pin_"+i);
			ps.setString(pi++, "os_ver");
			ps.setInt(pi++,0 );
			ps.setTimestamp(pi++, new Timestamp(System.currentTimeMillis()));
			ps.execute();
			ps.close();
		}
		con.commit();
		con.close();
		
	}
	@After
	public void tearDown() throws Exception {
		Connection con = getDerbyCon();
		con.createStatement().execute("delete from devices");
		con.commit();
	}
	@Test
	public void deviceMigration() throws Exception {
		prep("dev",100);
		
		Connection con = getDerbyCon();
		java.sql.ResultSet dbrs = con.createStatement().executeQuery("select * from devices");
		while(dbrs.next()) {
			System.out.println("got one");
		}
		
		XMLConfig config = (XMLConfig)JAXBUtil.unmarshalXmlFile("migration/device-mapping.xml", XMLConfig.class);
		config.setJdbcUrl(jdbcUrl);
		config.setJdbcDriver(jdbcDriver);
		config.setBatchWrites(1);
		
		RSExecutor executor = new RSExecutor(config);
		executor.execute();
		
		//find how many got in cassandra
		Session s = getSession(keyspaces[0]);
		ResultSet rs = s.execute("select count(*) from devices");
		assertEquals(100, rs.one().getLong(0));
		
		
	}
	
	class TPExec extends ThreadPoolExecutor {

		public TPExec(int corePoolSize, int maximumPoolSize,
				long keepAliveTime, TimeUnit unit,
				BlockingQueue<Runnable> workQueue) {
			super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
			
		}

		@Override
		protected void beforeExecute(Thread t, Runnable r) {
			
			//super.beforeExecute(t, r);
			System.out.println("Runnable: " + r);
		}
		
	}
	static class RSExecTestExt extends RSExecutor {

		
		public RSExecTestExt(XMLConfig config) throws ClassNotFoundException,
				SQLException {
			super(config);
			new Thread(new ResumeCheck()).start();
			
		}

		@Override
		protected void writeToCassandra(String keyspace,
				RegularStatement statement, int batchSize) {
			// TODO Auto-generated method stub
			super.writeToCassandra(keyspace, statement, batchSize);
			suspend();
		}
		class ResumeCheck implements Runnable {

			@Override
			public void run() {
				while (true) {
				if (ExecutorState.SUSPENDED == getExecutorState() ) {
					System.out.println("RESUMING!!");
					resume();
				}
				try {
					Thread.sleep(1000);
				}catch (InterruptedException e) {}
				}
				
			}
			
		}
		
		
	}
	
	@Test
	public void migrationStatesTest() throws Exception { 
		prep("dev",100);
		
		XMLConfig config = (XMLConfig)JAXBUtil.unmarshalXmlFile("migration/device-mapping.xml", XMLConfig.class);
		config.setJdbcUrl(jdbcUrl);
		config.setJdbcDriver(jdbcDriver);
		config.setBatchWrites(100);
		
		RSExecutor executor = new RSExecTestExt(config);
		
		
		ExecutorState state = executor.getExecutorState();
		assertEquals(ExecutorState.INIT,state);
		executor.execute();
		
		Session s = getSession(keyspaces[0]);
		ResultSet rs = s.execute("select count(*) from devices");
		assertEquals(100, rs.one().getLong(0));
		
		
	}

}
