package driver.em;

import java.util.Collection;
import java.util.Map;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;



/**
 * Stripped down {@link javax.persistence.EntityManager}.
 * that supports Cassandra CQL java driver storage 
 * 
 * See {@link ReqConstants} for parameter explanations
 *
 *
 * @param <K> - the key of the entity
 * @param <E> - the entity 
 */
public interface EntityManager<K, E>{

	
	/**
	 * Store data into storage
	 * 
	 * @param entity                    The entity object containing data to store
	 * @param requestParameters         Additional request parameters that may carry EntityManager implementation specific configuration
	 *                                  for ex. Cassandra Consistency Level, Retry Policy, etc.
	 */
	void persist(E entity, Map<String, Object> requestParameters);
	
	/**
	 * Remove data from storage
	 * 
	 * @param key                       The row key of the entity to remove
	 * @param requestParameters         Additional request parameters that may carry EntityManager implementation specific configuration
	 *                                  for ex. Cassandra Consistency Level, Retry Policy, etc.
	 */
	void remove(K key, Map<String, Object> requestParameters);
	
	/**
	 * Find an entity in the storage by its key.
	 * 
	 * @param key                       The row key of the entity to find
	 * @param requestParameters         Additional request parameters that may carry EntityManager implementation specific configuration
	 *                                  for ex. Cassandra Consistency Level, Retry Policy, etc.
	 * @return                          The retrieved entity object or null if none is found
	 */
	E find(K key, Map<String, Object> requestParameters);
	
	/**
	 * The equivalent of a statement executed with result set returned
	 */
	Collection<E> findBy(String query,Map<String, Object> requestParameters );
			
	/**
	 * The equivalent of a statement executed with bind variables values
	 * This may in fact be far to generic for end consumption, but throw it in there
	 * for now.
	 * 
	 * @param query
	 * @param values - the values to bind to query
	 * @return
	 */
	Collection<E> findBy(String query, Object[] values,Map<String, Object> requestParameters);
	
	void executeBatch(BatchStatement bs,Map<String, Object> requestParameters);
	
	/**
	 * Builds an UPDATE or INSERT statement that can be used
	 * by {@link #executeBatch(BatchStatement)}, or other context
	 * 
	 * @param entity
	 * @return
	 */
	Statement persistStatement(E entity,Map<String, Object> requestParameters);
	
}

