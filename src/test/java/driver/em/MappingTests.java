package driver.em;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;


public class MappingTests {

	Map<String, String> sampleMap;
	
	@Before
	public void before() {
		sampleMap = new HashMap<>();
		sampleMap.put("onekey","oneValue");
		sampleMap.put("twokey","twoValue");
		
	}
	@Test
	public void test() {
		SampleEntity sample = new SampleEntity("ID");
		sample.simpleCol = "col1value";
		
		EntityConfig config = new EntityConfig(sample.getClass());
		
		config.discover();
		
		System.out.println(config.getIdPredicate());
	}
	
	@Test
	public void testUpdateStatement() throws Exception {
		SampleEntity sample = new SampleEntity("ID");
		sample.simpleCol = "col1value";
		sample.properties = sampleMap;
		
		EntityConfig config = new EntityConfig(sample.getClass());
		
		config.discover();
		
		SimpleStatement ss = config.getUpdateStatement(sample);
		
		System.out.println(ss.getQueryString());
		
	}
	
	@Test
	public void testEmbedded() throws Exception {
		SampleEmbeddedEntity sample = new SampleEmbeddedEntity(new SampleEmbeddedEntity.Id("A","B"));
		sample.properties = sampleMap;
		EntityConfig config = new EntityConfig(sample.getClass());
		
		config.discover();
		
		System.out.println(config.getIdPredicate());
		
		SimpleStatement ss = config.getUpdateStatement(sample);
		
		System.out.println(ss.getQueryString());
		
	}
	
	@Test
	public void testByteBuffer() throws Exception {
		class Test {
			public String afield;
		}
		
		class TestStatement extends SimpleStatement {
			
			ByteBuffer []bb = null;
			
			public TestStatement(String query) {
				super(query);
				
			}

			@Override
			public ByteBuffer getRoutingKey() {
				// TODO Auto-generated method stub
				return super.getRoutingKey();
			}
			public ByteBuffer[] getRoutingKeys() {
				return bb;
			}
			@Override
			public SimpleStatement setRoutingKey(
					ByteBuffer... routingKeyComponents) {
				bb = routingKeyComponents;
				return super.setRoutingKey(routingKeyComponents);
			}
			
			
		}
		ColumnMapping mapping = new ColumnMapping("colname", Test.class.getField("afield"));
		mapping.getBuffer("somevalue");
		
		DefaultEntityManager<SampleEmbeddedEntity.Id,SampleEmbeddedEntity> defEntityMgr = new DefaultEntityManager<>(null, SampleEmbeddedEntity.class);
		
		TestStatement statement = new TestStatement("NOT VALID STATEMENT");
		SampleEmbeddedEntity sample = new SampleEmbeddedEntity(new SampleEmbeddedEntity.Id("A","B"));
		
		SampleEmbeddedEntity.Id idObj = defEntityMgr.getIdValue(sample);
		Assert.assertEquals(sample.id, idObj);
		
		defEntityMgr.setRouting(statement, idObj);
		Assert.assertEquals(2, statement.getRoutingKeys().length );
		statement.getRoutingKey();
		
	}
}
