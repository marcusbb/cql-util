package reader;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import driver.em.CUtils;
import driver.em.CassConfig;
import driver.em.Composite;
import driver.em.DefaultEntityManager;
import driver.em.SampleEntity;
import driver.em.TestBase;

/**
 * 
 *  
 * 
 */
public class ReaderTests extends TestBase {

	static CQLRowReader reader = null;
	
	static Session session = null;
	
	
	@BeforeClass
	public static void bc() throws Exception {
		TestBase.beforeClass();
		TestBase.loadCql("reader/schema.cql", "icrs");
		session = TestBase.session;
	}

	@Before
	public void before() throws Exception {
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("reader-config.xml");
		reader = new CQLRowReader(new Stubs.MyReaderJob());
		reader.config = (ReaderConfig) unmarshaller.unmarshal(ins);
		reader.config.setTable("devices");
		PKConfig.ColumnInfo []partitionkey = {new PKConfig.ColumnInfo("id", DataType.ascii())};
		PKConfig.ColumnInfo []clusterkey = {new PKConfig.ColumnInfo("id", DataType.ascii())};
		
		PKConfig pk = new PKConfig(partitionkey, clusterkey);
		reader.config.setPkConfig(pk);
		
		reader.session = session;
	
		ResultSet rs = session.execute("select id from devices");
		for (Row row:rs.all()) {
			SimpleStatement ss = new SimpleStatement("delete from devices where id = ?", row.getString(0));
			session.execute(ss);
		}
				
	}

	
	// put in a number less than the page size
	@Test
	public void testUnderFlow() {
		reader.config.setPageSize(1001);
		insertSeqDev(100);

		reader.read();
		Assert.assertEquals(new Long(100), reader.getTotalReadCount());
	}

	
	@Test
	public void testOverFlowSize5Page12() {
		insertSeqDev(12);
		reader.config.setPageSize(5);
		reader.read();
		Assert.assertEquals(new Long(12), reader.getTotalReadCount());
	}
	@Test
	public void testOverFlowPage100Size150() {
		insertSeqDev(150);
		reader.config.setPageSize(100);
		reader.read();
		Assert.assertEquals(new Long(150), reader.getTotalReadCount());
	}

	// larger but a modulus of page size
	@Test
	public void testOverFlowPage100() {
		insertSeqDev(1000);
		reader.config.setPageSize(100);
		reader.read();
		Assert.assertEquals(new Long(1000), reader.getTotalReadCount());
	}

	@Test
	public void testRowAndCol() {
		//500 cql rows
		insertSeqDev2(100, 5);
		reader.config.setPageSize(100);
		reader.read();
		Assert.assertEquals(new Long(100*5), reader.getTotalReadCount());
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