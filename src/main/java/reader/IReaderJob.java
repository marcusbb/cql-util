package reader;

import com.datastax.driver.core.Row;

/**
 * 
 * A ReaderJob is responsible for creating {@link RowReaderTask} which 
 * are the end consumers of reading a Cassandra row {@link Row}.
 * 
 * The result V is called from the result of this computation and 
 * can be handled by the client via {@link #processResult(Object)}
 * 
 * The job is also notified via {@link #onReadComplete()} 
 * when the read entire data set (or what has been configured)
 *  
 * 
 * @param <V>
 */
public interface IReaderJob<V> {

	public abstract RowReaderTask<V> newTask() throws Exception;
	
	public abstract void processResult(V result);
	
	/**
	 * The read job is complete
	 */
	public abstract void onReadComplete();
}
