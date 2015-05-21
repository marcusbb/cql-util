package driver.em;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class DefaultBatchManager implements BatchManager {

	Map<Class<?>,EntityManager<?,?>> emMap = new HashMap<Class<?>, EntityManager<?,?>>();
	private Session session;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public DefaultBatchManager(Session session,Class<?>...types) {
		this.session = session;
		for (Class<?> c:types) {
			emMap.put(c,new DefaultEntityManager(session, c));
		}
		
	}
	
	private BatchStatement build(Map<String,Object> params,Object...entities) {
		BatchStatement bs = new BatchStatement();
		
		for (Object entity:entities) {
			
			@SuppressWarnings("unchecked")
			EntityManager<Object,Object> em = (EntityManager<Object, Object>) emMap.get(entity.getClass());
			if (em == null)
				throw new IllegalArgumentException("Entity type " + entity.getClass() + "isn't defined");
		
			bs.add(em.persistStatement(entity,params));
			
			
		}
		return bs;
	}
	
	@Override
	public void batchWrite(Map<String,Object> params, Object... entities) {
		
		
		session.execute(build(params, entities));
		
	}
	
	@Override
	public ResultSetFuture batchWriteAsync(Map<String,Object> params, Object... entities) {
		ResultSetFuture f = session.executeAsync(build(params, entities));
		
		return f;
	}
	
	

}
