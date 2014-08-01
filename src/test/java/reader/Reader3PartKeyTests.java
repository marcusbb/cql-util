package reader;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import driver.em.CUtils;
import driver.em.CassConfig;
import driver.em.Composite;
import driver.em.DefaultEntityManager;
import driver.em.SampleEntity;
import driver.em.TestBase;

/**
 * 
 * this needs to be run:
 * 
 * 
 */
public class Reader3PartKeyTests {

	static CQLRowReader reader = null;
	static Session session = null;
	static Cluster cluster = null;
	
	@BeforeClass
	public static void beforeClass() throws Exception {

		cluster = CUtils.createCluster(new CassConfig());
		session = cluster.connect("icrs");	
	}

	@Before
	public void before() throws Exception {
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("reader-config2.xml");
		reader = new CQLRowReader(new Stubs.MyReaderJob());
		reader.config = (ReaderConfig) unmarshaller.unmarshal(ins);

		reader.cluster = cluster;
		reader.session = session;
		
		
		try {
			session.execute("drop table devices2");
		}catch (Exception e) {
			//this is ok
		}
		
		session.execute("create table devices2 (  id text, type text, name text, value text,  PRIMARY KEY (id, type, name) )");
	}

	// put in a number less than the page size
	@Test
	public void testUnderFlow() {
		reader.config.setPageSize(1001);
		insertSeqDev(100);

		reader.read();

	}

	
	@Test
	public void testOverFlowSize5Page12() {
		insertSeqDev(12);
		reader.config.setPageSize(5);
		reader.read();
	}
	@Test
	public void testOverFlowPage100Size150() {
		insertSeqDev(150);
		reader.config.setPageSize(100);
		reader.read();
	}

	// larger but a modulus of page size
	@Test
	public void testOverFlowPage100() {
		insertSeqDev(1000);
		reader.config.setPageSize(100);
		reader.read();
	}

	@Test
	public void testRowAndCol() {
		//500 cql rows
		insertSeqDev2(100, 5);
		reader.config.setPageSize(100);
		reader.read();
		
	}
	
	
	public void insertSeqDev(int num) {
		DefaultEntityManager<Device2.Id, Device2> em = new DefaultEntityManager<>(
				session, Device2.class);
		for (int i = 0; i < num; i++) {
			Device2 entity = new Device2("id-" + i, "name-" + i,"type-"+i, "val-" + i);
			em.persist(entity, CUtils.getDefaultParams());
		}
	}
	public void insertSeqDev2(int rowNum,int colCount) {
		DefaultEntityManager<Device2.Id, Device2> em = new DefaultEntityManager<>(
				session, Device2.class);
		for (int i = 0; i < rowNum; i++) {
			for (int j=0;j<colCount;j++) {
				Device2 entity = new Device2("id-" + i, "name-" + j,"type-"+j, "val-" + j);
				em.persist(entity, CUtils.getDefaultParams());
			}
		}
	}
	
	
	@Test
	public void testByteBufferEquality() {
		String one = "one";
		
		ByteBuffer bb = Composite.toByteBuffer(new Object[]{"one","two"});
		ByteBuffer bb2 = Composite.toByteBuffer(new Object[]{ByteBuffer.wrap(one.getBytes()),"two"});
		ByteBuffer bb3 = Composite.toByteBuffer(new Object[]{"two","one"});
		bb2.limit();bb2.array();
		
		//bb3.flip();
		Assert.assertTrue(bb.equals(bb2));
		Assert.assertFalse(bb.equals(bb3));
		
	}
}