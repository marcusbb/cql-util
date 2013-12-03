package driver.em;

import static org.junit.Assert.*;

import org.junit.Test;

public class IdOnlyTests extends TestBase {


	String cqlCreate2 = "CREATE TABLE id_only (first text, second bitint, primary key(first,second))";
	String cqlDrop2 = "DROP TABLE id_only";
	
	//this tests the insert only functionality
	@Test
	public void testIdOnly() {
		DefaultEntityManager<IdOnlyEntity.Id, IdOnlyEntity> em = new DefaultEntityManager<>(session, IdOnlyEntity.class);
		
		em.persist(new IdOnlyEntity(new IdOnlyEntity.Id("firstVal",0L)), CUtils.getDefaultParams());
		
	}

}
