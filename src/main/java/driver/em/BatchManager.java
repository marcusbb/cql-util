package driver.em;

import java.util.Map;

import com.datastax.driver.core.ResultSetFuture;

/**
 * 
 * Abstraction of the batching 
 *
 */
public interface BatchManager {

	void batchWrite(Map<String,Object> params, Object...entities);
	
	ResultSetFuture batchWriteAsync(Map<String,Object> params, Object...entities);
	
	
}
