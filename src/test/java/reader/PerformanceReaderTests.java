package reader;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;

import driver.em.CUtils;
import driver.em.DefaultEntityManager;

/**
 * 
 * this needs to be run:
 * 
 * 
 */
public class PerformanceReaderTests {

	static CQLRowReader reader = null;
	static Session session = null;

	@BeforeClass
	public static void beforeClass() throws Exception {

		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("perf-reader-config.xml");
		reader = new CQLRowReader();
		reader.config = (ReaderConfig) unmarshaller.unmarshal(ins);

		reader.cluster = CUtils.createCluster(reader.config.getCassConfig());
		reader.session = reader.cluster.connect("icrs");
		session = reader.session;
	}

	//@Before
	public void before() {
		try {
			session.execute("drop table devices");
		} catch (Exception e) {
			// this is OK
		}
		session.execute("create table devices (  id text, name text, value text,  value_ascii ascii, value_d bigint,  PRIMARY KEY (id, name) )");

	}

	
	@Test
	public void testPerform100K() {
		//insertSeqDev(100000);
		
		reader.read();
		
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