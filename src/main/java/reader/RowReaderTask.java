package reader;

import com.datastax.driver.core.Row;

//A row reader task
public interface RowReaderTask<V> extends java.util.concurrent.Callable<V> {

	
	public void process(Row row,RowKey rowKey);
	
}
