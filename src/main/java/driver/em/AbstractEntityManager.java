package driver.em;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;

import static driver.em.CharConst.*;

/**
 * Provides default support for entity mapping,
 *  {@link #persist(Object, Map)}
 *  {@link #find(Object, Map)}
 *  {@link #remove(Object, Map)}
 *  
 *  See explanation of {@link ReqConstants}, to determine the properties 
 *  of a driver operation.  
 *  Thinking of batch support.
 * 
 * Limited support for {@link Entity} mappings
 * Limited supported for jpa entity mapping.  If anything, they are convenient nomenclature.
 * {@link Id}
 * {@link EmbeddedId}
 * 
 * 
 */
public abstract class AbstractEntityManager<K,E> implements EntityManager<K, E> {

	//driver session
	Session session;
	
	
	//not the ideal place to put this
	//that's why it's public as it should be moved out
	//not sure where yet
	public static EntityEMMap mapping = new EntityEMMap();
	
	
	private EntityConfig<E> entityConfig;
	
	public AbstractEntityManager(Session session) {
		this.session = session;
	}
	
	public AbstractEntityManager(Session session, Class<E> entityClass) {
		
		this(session);
		if (entityClass.isAnnotationPresent(Entity.class)) {
			this.entityConfig = mapping.getConfig(entityClass);	
		}
		
	}
	
	
	
	
	
	@Override
	public void persist(E entity, Map<String, Object> requestParameters) {
		
		if (entityConfig != null) {
			
			//build and execute the statement
			SimpleStatement ss = getUpdateStatement(entity);
			setRouting(ss, getIdValue(entity));
			getSession().execute( ss );
			
		}
		else {
			//throw exception
			throw new UnsupportedOperationException("The operation must be overridden");
		}
		
	}


	@Override
	public void remove(K key, Map<String, Object> requestParameters) {
		if (entityConfig != null) {
			
			//build and execute the statement
			SimpleStatement ss = entityConfig.getDelStatement(key);
			setRouting(ss, key);
			getSession().execute( ss );
			
		}
		else {
			//throw exception
			throw new UnsupportedOperationException("The operation must be overridden");
		}
		
	}


	@Override
	public E find(K key, Map<String, Object> requestParameters) {

		if (entityConfig != null) {
			
			//build and execute the statement
			SimpleStatement ss = entityConfig.getAllQuery(key);
			setRouting(ss, key);
			ResultSet result = getSession().execute(
				ss
			);
			Iterator<Row> resultIter = result.iterator();
			
			if (!resultIter.hasNext())
				return null;
			
			E entity = null;
			try {
				entity = entityConfig.get(resultIter.next() );
			} 
			catch (Exception e) {
				throw new PersistenceException("Entity exception: " + e.getMessage());
			}
			
			
			return entity;
		}
		else {
			//throw exception
			throw new UnsupportedOperationException("The operation must be overridden");
		}
		
	}


	@Override
	public void executeBatch(BatchStatement bs) {
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * 
	 * Query methods are generic as long as type can be inferred.
	 * And implementors can provide the necessary mapping to fields.
	 */


	@Override
	public Collection<E> findBy(String query,Map<String, Object> requestParameters) {
		
		return findBy(query, null,requestParameters);
	}

	@Override
	public Collection<E> findBy(String query, Object[] values,Map<String, Object> requestParameters) {
		ArrayList<E> list = new ArrayList<>();
		SimpleStatement ss = new SimpleStatement(query,values);
		
		ResultSet result = session.execute(ss);
		
		Iterator<Row> resultIter = result.iterator();
		
		if (!resultIter.hasNext())
			return null;
		
		E entity = entityConfig.get(resultIter.next() );
		
		list.add(entity);
		
		return list;
	}
	
	protected void defineParams(Statement statement, Map<String, Object> params) {
		//there are a couple of other policies to set 
		//but consistency is likely the only one required.
		statement.setConsistencyLevel((ConsistencyLevel) params.get(ReqConstants.CONSISTENCY));
		statement.setRetryPolicy((RetryPolicy) params.get(ReqConstants.RETRY_POLICY));
		
	}
	/**
	 * Prepare map for CQL statement "UPDATE" semantics.
	 * for a simple statement, to complete the {@link #addMapValuesToList(List, Map)}
	 * must be called
	 * 
	 * We can combine the 2 methods, but construction of query becomes more restrictive.
	 * 
	 * @param mapName
	 * @param map
	 * @return
	 */
	protected String prepareMap(String mapName,Map<String,String> map) {
		StringBuilder builder = new StringBuilder();
		int i = 0;
		int size = map.size();
		for (String key:map.keySet()) {
			builder.append(mapName).append(openB).append(sq)
				.append(key)
				.append(sq).append(closeB)
				.append(space)
				.append(eq)
				.append(space)
				.append(ques);
			
			if (i != (size-1) )
				builder.append(comma);
			i++;
		}
		
		return builder.toString();
	}
	/**
	 * Critical: key set is in order, to the above method {@link #prepareMap(String, Map)}
	 * 
	 * @param list
	 * @param map
	 */
	protected void addMapValuesToList(List<Object> list,Map<String,String> map) {
		
		for (String key:map.keySet()) {
			list.add(map.get(key));
		}
	}
	protected K getIdValue(E entity) {
		K idObj = null;
		if (entityConfig.embedded == null) {
			idObj = (K)entityConfig.idMapping.get(entity);
		}else {
			idObj = (K)entityConfig.embedded.get(entity);
		}
		return idObj;
	}
	/**
	 * 
	 * @param statement
	 */
	public void setRouting(SimpleStatement statement,K idObj) {
		
		if (entityConfig.embedded == null) {
			
			statement.setRoutingKey(entityConfig.idMapping.getBuffer(idObj));
		}
		//else a embedded key
		else {
			
			ByteBuffer []bb = new ByteBuffer[entityConfig.embedded.columns.length];
			int i = 0 ;
			for (ColumnMapping mapping:entityConfig.embedded.columns) {
				bb[i++] = mapping.getBuffer(mapping.get(idObj));
			}
			statement.setRoutingKey(bb);
		}
		
	}
	
	/**
	 * This was moved from EntityConfig 
	 * 
	 * 
	 * @param obj
	 * @return
	 */
	protected SimpleStatement getUpdateStatement(E obj )  {
		
		EntityConfig<E> ec = this.entityConfig;
		StringBuilder builder = new StringBuilder("UPDATE ").append(ec.tableName).append(" SET ");
		ArrayList<Object> valueList = new ArrayList<>();
		int i = 0;
		//go through the columns
		
		for (String col:ec.colsToFields.keySet()) {
			
			ColumnMapping mapping = ec.colsToFields.get(col);
			Object valueObj = mapping.get(obj);
			
			
			if (!mapping.isMap) {
				builder.append(ec.getColUpdate(col) );
				valueList.add(valueObj);
				
			} else {
				//this is a fairly large gap in functionality
				//as we only support <String,String> maps
				Map<String,String> map = (Map<String,String>)valueObj;
				if (map != null) {
					int im = 0;
					for (String key:map.keySet()) {
						builder.append(col).append(openbsq).append(key).append(sqcloseb).append( eqParam);
						if (im++ < map.size() -1)
							builder.append(comma).append(space);
						
						valueList.add(map.get(key));
						
					}
				}
				
			}
			if (i++ < (ec.colsToFields.size() -1)) 
				builder.append(comma).append(space);
			
			
		}
	
		builder.append(ec.getIdPredicate());
		if (ec.embedded != null) {
			try {
				//this needs some improvement to the embedded class
				Object idObj = ec.embedded.field.get(obj);
				for (ColumnMapping membed:ec.embedded.columns) {
					valueList.add(membed.get(idObj) );
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		} 
		//non-composite PK
		else {
			Object idObj = ec.idMapping.get(obj);
			valueList.add(idObj);
		}
		
		SimpleStatement ss = new SimpleStatement(builder.toString(),valueList.toArray());
		//System.out.println("update ss : " + ss.getQueryString());
		return ss;
	}
	
	public Session getSession() {
		return session;
	}
}
