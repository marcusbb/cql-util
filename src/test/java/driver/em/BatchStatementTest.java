package driver.em;

import static org.junit.Assert.fail;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSetFuture;

public class BatchStatementTest extends TestBase {

	@Test
	public void testBatchUpdate() {
		SampleEntity entity1 = new SampleEntity("An_id");
		entity1.date = new Date();
		
		SampleEntity entity2 = new SampleEntity("An_id2");
		entity2.date = new Date();
		
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
				
		
		BatchStatement bs = new BatchStatement();
		bs.add(em.persistStatement(entity1));
		bs.add(em.persistStatement(entity2));
		em.executeBatch(bs);
		
		Assert.assertNotNull(em.find("An_id"));
		Assert.assertNotNull(em.find("An_id2"));
		
	}
	
	
	@Test
	public void testBatchManagerUpdate() {
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
		SampleEntity entity1 = new SampleEntity("An_id3");
		entity1.date = new Date();
		
		SampleEntity entity2 = new SampleEntity("An_id4");
		entity2.date = new Date();
		
		BatchManager bm = new DefaultBatchManager(session, SampleEntity.class);
		bm.batchWrite(CUtils.getDefaultParams(), entity1,entity2);
		
		Assert.assertNotNull(em.find("An_id3"));
		Assert.assertNotNull(em.find("An_id4"));
		
	}
	@Test
	public void testBatchManagerAsync() throws ExecutionException, InterruptedException {
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
		SampleEntity entity1 = new SampleEntity("An_id3_async");
		entity1.date = new Date();
		
		SampleEntity entity2 = new SampleEntity("An_id4_async");
		entity2.date = new Date();
		
		BatchManager bm = new DefaultBatchManager(session, SampleEntity.class);
		ResultSetFuture f = bm.batchWriteAsync(CUtils.getDefaultParams(), entity1,entity2);
		f.get();
		
		Assert.assertNotNull(em.find("An_id3_async"));
		Assert.assertNotNull(em.find("An_id4_async"));
		
	}
	
	@Test
	public void testBatchManagerMixed() {
		DefaultEntityManager<String, SampleEntity> em = new DefaultEntityManager<>(session, SampleEntity.class);
		DefaultEntityManager<IdOnlyEntity.Id, IdOnlyEntity> em2 = new DefaultEntityManager<>(session, IdOnlyEntity.class);
		SampleEntity entity1 = new SampleEntity("An_id5");
		entity1.date = new Date();
		
		IdOnlyEntity e2 = new IdOnlyEntity(new IdOnlyEntity.Id("id",1L));
		
		BatchManager bm = new DefaultBatchManager(session, SampleEntity.class,IdOnlyEntity.class);
		bm.batchWrite(CUtils.getDefaultParams(), entity1,e2);
		
		Assert.assertNotNull(em.find("An_id5"));
		Assert.assertNotNull(em2.find(new IdOnlyEntity.Id("id",1L)));
		
		
	}
	
	

}
