package reader;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import reader.Device.Id;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
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
 * CREATE TABLE wide_row (
  id text,
  name text,
  value text,
  PRIMARY KEY ((id), name)
)
 * 
 * 
 */
public class WideRowTests {

	
	static Session session = null;
	static JobBootStrap boot = null;
	
	@BeforeClass
	public static void beforeClass() throws Exception {

		boot = new JobBootStrap() {
			
			@Override
			public ReaderJob<?> initJob(ReaderConfig readerConfig) {
				return new WideRowJob();
			}
		};
		boot.bootstrap(ReaderConfig.class.getName(), Thread.currentThread().getContextClassLoader().getResourceAsStream("wide-config.xml"));
		
	}
	
	static class WideRowJob extends ReaderJob<Void> {

		@Override
		public RowReaderTask<Void> newTask() throws Exception {
			return new RowReaderTask<Void>() {
				
				@Override
				public Void process(Row row, ColumnDefinitions colDef,
						ExecutionInfo execInfo) {
					// TODO Auto-generated method stub
					return null;
				}
			};
		}

		@Override
		public void processResult(Void result) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onReadComplete() {
			// TODO Auto-generated method stub
			
		}
		
	}
	@Entity
	@Table(name="wide_row")
	public static class WideRow {
		public WideRow() { }
		public WideRow(String id,String name,String value) {
			this.id = new Id();
			this.id.id = id;
			this.id.name = name;
			this.value = value;
		}
		@EmbeddedId
		public Id id;
		
		public static class Id {
			@Column(name="id")
			public String id;
			@Column(name="name")
			public String name;
		}
		@Column(name="value")
		public String value;
	}

	
	//@Test
	public void testPerform100K() {
		insertSeqDev2(1,100000);
		
		//reader.read();
		
	}
	@Test
	public void read() {
		boot.runJob();
		
	}
	public void insertSeqDev2(int rowNum,int colCount) {
		DefaultEntityManager<WideRow.Id, WideRow> em = new DefaultEntityManager<>(
				session, WideRow.class);
		Random rand = new Random();
		int pref = rand.nextInt();
		System.out.println("Rand prefix: " + pref);
		for (int i = 0; i < rowNum; i++) {
			for (int j=0;j<colCount;j++) {
				WideRow entity = new WideRow("id-" +pref+"-"+ i, "name-" + j, "val-" + j);
				em.persist(entity, CUtils.getDefaultParams());
			}
		}
	}
	
	
}