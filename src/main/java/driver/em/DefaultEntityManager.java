package driver.em;

import com.datastax.driver.core.Session;

/**
 * 
 * remove the need for templating your entity managers from {@link AbstractEntityManager}
 *
 * @param <K>
 * @param <E>
 */
public class DefaultEntityManager<K,E> extends AbstractEntityManager<K, E> {

	public DefaultEntityManager(Session session,Class<E> entityClass) {
		super(session,entityClass);

	}

}
