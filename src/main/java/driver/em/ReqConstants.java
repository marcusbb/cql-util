package driver.em;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision;

public enum ReqConstants {

	//The requests parameters
	//are really just an abstraction to the 
	//details of the driver, if we wanted to peel this off
	
	/**
	 * Abstracted {@link ConsistencyLevel}
	 */
	CONSISTENCY, 
	/**
	 * Abstracted {@link RetryPolicy}
	 * default  is {@link RetryDecision#rethrow()}
	 * 
	 */
	RETRY_POLICY,
	
	/**
	 * Use prepared statement cache
	 */
	PREPARED_CACHE;
}
