package driver.em;

import java.util.Collection;
import java.util.Map;

import com.datastax.driver.core.Session;

/**
 * 
 * remove the need for templating your entity managers from {@link AbstractEntityManager}
 *
 * @param <K>
 * @param <E>
 */
public class DefaultEntityManager<K,E> extends AbstractEntityManager<K, E> {

	private Map<String,Object> defaultRequestParameters;
	
	public DefaultEntityManager(Session session,Class<E> entityClass) {
		super(session,entityClass);
		this.defaultRequestParameters = CUtils.getDefaultParams();
	}
	
	public DefaultEntityManager(Session session,Class<E> entityClass,Map<String,Object> requestParameters) {
		this(session,entityClass);
		
	}
	
	public void persist(E entity) {
		super.persist(entity, defaultRequestParameters);
	}
	
	public void remove(K key) {
		super.remove(key, defaultRequestParameters);
	}
	
	public E find(K key) {
		return super.find(key, defaultRequestParameters);
	}
	
	public Collection<E> findBy(String query) {
		return super.findBy(query, defaultRequestParameters);
	}
	
	public Collection<E> findBy(String query, Object[] values) {
		return super.findBy(query, values, defaultRequestParameters);
	}

	
}
