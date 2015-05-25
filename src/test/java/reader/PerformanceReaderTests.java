package reader;

import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import driver.em.CUtils;
import driver.em.DefaultEntityManager;
import driver.em.TestBase;

/**
 * 
 * this needs to be run:
 * 
 * 
 */
public class PerformanceReaderTests extends TestBase {

	static CQLRowReader reader = null;
	static Session session = null;

	
	
	@BeforeClass
	public static void bc() throws Exception {
		TestBase.beforeClass();
		TestBase.loadCql("reader/schema.cql", "icrs");
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("perf-reader-config.xml");
		reader =  new CQLRowReader(new Stubs.MyReaderJob());
		reader.config = (ReaderConfig) unmarshaller.unmarshal(ins);

		reader.cluster = CUtils.createCluster(reader.config.getCassConfig());
		reader.session = reader.cluster.connect("icrs");
		session = reader.session;
	}
	
	protected static CQLRowReader getAndInit(String xmlConfig) throws Exception {
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(xmlConfig);
		CQLRowReader r = new CQLRowReader(new Stubs.MyReaderJob());
		r.config = (ReaderConfig) unmarshaller.unmarshal(ins);
		r.cluster = CUtils.createCluster(reader.config.getCassConfig());
		r.session = reader.cluster.connect("icrs");
		
		return r;
	}
	
	protected static CQLRowReader getAndInit(String xmlConfig,Session session) throws Exception {
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(xmlConfig);
		CQLRowReader r = new CQLRowReader(new Stubs.MyReaderJob());
		r.config = (ReaderConfig) unmarshaller.unmarshal(ins);

		r.session = session;
		return r;
	}

	

	
	
	@Test
	public void testRead() throws Exception {
		insertSeqDev(100000, 10);
		
		reader.read();
		
	}
	
	@Test
	public void testParallelRead() throws Exception {
		//split the load in 2
		reader.config.setEndToken(0L);
		
		final CQLRowReader reader2 = getAndInit("perf-reader-config.xml",session);
		reader2.session = session;
		reader2.config.setStartToken(1L);
		
		ExecutorService exec = Executors.newFixedThreadPool(2);
		
		final class ReaderTask implements Runnable{
			final CQLRowReader reader;
			ReaderTask(CQLRowReader reader) {
				this.reader = reader;
			}
			@Override
			public void run() {
				reader.read();
				System.out.println("*********Completed: " + reader.getTotalReadCount());
			}
			
		}
		exec.submit(new ReaderTask(reader));
		
		exec.submit(new ReaderTask(reader2));
		
		exec.shutdown();
		
		exec.awaitTermination(10, TimeUnit.MINUTES);
	}
	
	public void insertSeqDev(int numRows,int numThreads) throws InterruptedException {
		ExecutorService exec = Executors.newFixedThreadPool(numThreads);
		final int each = numRows/numThreads;
		final Random rand = new Random();
		for (int i=0;i<numThreads;i++)
			exec.submit(new Runnable() {
				
				@Override
				public void run() {
					DefaultEntityManager<Device.Id, Device> em = new DefaultEntityManager<>(
							session, Device.class);
					int pref = rand.nextInt();
					for (int i = 0; i < each; i++) {
						Device entity = new Device("id-" + pref+ i, "name-" + i, "val-" + i);
						em.persist(entity, CUtils.getDefaultParams());
					}
					
				}
			});
		exec.shutdown();
		exec.awaitTermination(10, TimeUnit.MINUTES);
	}
	
	public void insertSeqDev(int num) {
		DefaultEntityManager<Device.Id, Device> em = new DefaultEntityManager<>(
				session, Device.class);
		for (int i = 0; i < num; i++) {
			Device entity = new Device("id-" + i, "name-" + i, "val-" + i);
			em.persist(entity, CUtils.getDefaultParams());
		}
	}
	public void insertSeqDev2(int rowNum,int colCount) {
		DefaultEntityManager<Device.Id, Device> em = new DefaultEntityManager<>(
				session, Device.class);
		for (int i = 0; i < rowNum; i++) {
			for (int j=0;j<colCount;j++) {
				Device entity = new Device("id-" + i, "name-" + j, "val-" + j);
				em.persist(entity, CUtils.getDefaultParams());
			}
		}
	}
	
	
	
}