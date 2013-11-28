package driver.em;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hold this statically somewhere.
 * 
 * @author msimonsen
 *
 */
public class EntityEMMap {

	private Map<Class<?>,EntityConfig> configMap;
	
	public EntityEMMap() {
		configMap = new ConcurrentHashMap<Class<?>, EntityConfig>();
	}
	
	public <T> EntityConfig<T> getConfig(Class<T> entityCl) throws RuntimeException {
	
		EntityConfig<T> config = configMap.get(entityCl);
		
		if (config == null) {
			config = new EntityConfig(entityCl);
			config.discover();
			if (config != null)
				configMap.put(entityCl, config);
		}
		
		return config;
	}
	

	public Map<Class<?>, EntityConfig> getConfigMap() {
		return configMap;
	}

	public void resetMap() {
		configMap = new ConcurrentHashMap<>();
	}
}
