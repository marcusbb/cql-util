package driver.em;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

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

}
