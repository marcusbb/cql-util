package driver.em;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * 
 * requires this to be run on the server
 * 
 * CREATE TABLE sample (
  key text,
  col1 text,
  col2 bigint,
  PRIMARY KEY (key)
) 
 *
 */
public class SampleEntityTests extends TestBase {

	String cqlCreate = "CREATE TABLE sample ( key text,  col1 text, col2 bigint, properties map<text,text>,date_col timestamp, b_info blob, PRIMARY KEY (key) ) ";
	String cqlDrop = "DROP TABLE sample";
	
	
	
	@Test
	public void test() {
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
		SampleEntity entity = new SampleEntity();
		entity.id = "someid";
		entity.ts = System.currentTimeMillis();
		entity.simpleCol = "simple";
		entity.properties = new HashMap<>();
		entity.properties.put("key1", "value1");
		entity.date = new Date();
		entity.blob = ByteBuffer.wrap(new byte[1024]);
		
		em.persist(entity, CUtils.getDefaultParams());
		
		SampleEntity found = em.find("someid", CUtils.getDefaultParams());
		
		Assert.assertNotNull(found);
		Assert.assertEquals("someid", found.id);
		Assert.assertEquals("simple", found.getSimpleCol());
		
		//just checks that set was called
		Assert.assertEquals("simple", found.mocked);
		
		Collection<SampleEntity> col = em.findBy("select col1 from sample where key = ?",new Object[]{"someid"}, CUtils.getDefaultParams());
		
		Assert.assertNotNull(col);
		
		
		//em.remove("someid", CUtils.getDefaultParams());
		
		
	}
	
	@Test
	public void testByteField() throws Exception {
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
		
		ByteBuffer bb = ByteBuffer.wrap(serialize("new") );
		
		em.session.execute("update sample SET b_info = ? where key = ?", bb,"blob_id");
		
		ResultSet rs = em.session.execute("select b_info from sample where key = ?", "blob_id");
		List<Row> rows = rs.all();
		for (Row row:rows) {
			ByteBuffer bb2 = row.getBytes("b_info");
			byte []b = new byte[bb2.remaining()];
			int i=0;
			while (bb2.remaining() >0)
				b[i++] = bb2.get();
				
			//
			
			String compare = (String)deserialize(b);
			org.junit.Assert.assertEquals("new", compare);
			
			
		}
	}
	
	@Test
	public void testBlobOnly() throws IOException,ClassNotFoundException {
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
		
		SampleEntity entity = new SampleEntity();
		entity.id = "someid2";
		
		entity.blob = ByteBuffer.wrap(serialize(new String("new")) );
		
		em.persist(entity, CUtils.getDefaultParams());
		
		SampleEntity found = em.find("someid2", CUtils.getDefaultParams());
		
		deserialize(found.blob);
		
		
	}

}
