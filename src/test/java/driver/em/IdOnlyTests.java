package driver.em;

import static org.junit.Assert.*;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;

public class IdOnlyTests extends TestBase {


	String cqlCreate2 = "CREATE TABLE id_only (first text, second bitint, primary key(first,second))";
	String cqlDrop2 = "DROP TABLE id_only";
	
	//this tests the insert only functionality
	@Test
	public void testIdOnly() {
		DefaultEntityManager<IdOnlyEntity.Id, IdOnlyEntity> em = new DefaultEntityManager<>(session, IdOnlyEntity.class);
		
		em.persist(new IdOnlyEntity(new IdOnlyEntity.Id("firstVal",0L)), CUtils.getDefaultParams());
		
	}
	
	@Test
	public void testPsCache() {
		DefaultEntityManager<IdOnlyEntity.Id, IdOnlyEntity> em = new DefaultEntityManager<>(session, IdOnlyEntity.class);
		IdOnlyEntity entity = new IdOnlyEntity(new IdOnlyEntity.Id("firstVal",0L));
		em.persist(entity, CUtils.getDefaultPSCacheParams());
		
		Map<String,PreparedStatement> cache = DefaultEntityManager.getCachedStatements();
		
		Assert.assertTrue(cache.containsKey(em.getInsertStatement(entity, true).getQueryString()));
		
	}

}
