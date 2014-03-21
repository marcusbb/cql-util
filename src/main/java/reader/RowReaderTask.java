package reader;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

/**
 * The task to implement in a job {@link ReaderJob}
 * 
 *
 *
 * @param <V> - return a result.
 */
public interface RowReaderTask<V>  {

	/**
	 * 
	 * @param row - the row being queried
	 * @param colDef - the column definition as defined by driver
	 * @param execInfo - the execution info provided by driver
	 * @return
	 */
	public V process(Row row,ColumnDefinitions colDef,ExecutionInfo execInfo);
	
}
