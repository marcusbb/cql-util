package reader.dist;

import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reader.ReaderConfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class DistReadTests {

	static HazelcastInstance[] hz;
	
	@Before
	public void before() {
		
	}
	@After
	public void after() {
		if (hz !=null)
		for (int i=0;i<hz.length;i++)
			hz[i].shutdown();
	}
	private static HazelcastInstance[] getInstances(int num) throws InterruptedException {
		final HazelcastInstance []insts = new HazelcastInstance[num];
		ExecutorService service = Executors.newCachedThreadPool();
		final AtomicInteger index = new AtomicInteger(0);
		for (int i=0;i<num;i++) {
			service.submit(new Runnable() {
				
				@Override
				public void run() {
					Config conf = new Config();
					
					conf.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
					TcpIpConfig tcpConf = new TcpIpConfig();
					//tcpConf.setConnectionTimeoutSeconds(1);
					tcpConf.setEnabled(true);
					tcpConf.addMember("127.0.0.1");
					conf.getNetworkConfig().getJoin().setTcpIpConfig(tcpConf);
					
					conf.setProperty("hazelcast.wait.seconds.before.join", "1");
					conf.setProperty("hazelcast.max.wait.seconds.before.join","1");
					//conf.setProperty("hazelcast.initial.min.cluster.size",Integer.toString(num));
					insts[index.getAndIncrement()] = Hazelcast.newHazelcastInstance(conf);
					
				}
			});
			
		}
		service.shutdown();
		service.awaitTermination(20, TimeUnit.SECONDS);
		Assert.assertEquals(num, insts[0].getCluster().getMembers().size());
		return insts;
		
	}
	@Test
	public void testSplit() throws Exception {
		hz = getInstances(5);
		
		DistReadCoordinator reader = new DistReadCoordinator(hz[0]);
		
		ReaderConfig [] configs = reader.getReaderConfig();
		
		for (int i=0;i<configs.length;i++) {
			System.out.println("Config: " + configs[i]);
		}
		Assert.assertEquals(Long.MIN_VALUE, (long)configs[0].getStartToken());
		Assert.assertEquals(Long.MAX_VALUE, (long)configs[configs.length-1].getEndToken());
		
		Assert.assertEquals(5, configs.length);
		
	}
	
	@Test
	public void test2() throws Exception {
		hz = getInstances(3);
		
	}

}
