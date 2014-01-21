package reader;

import com.datastax.driver.core.Row;

//A row reader task
public interface RowReaderTask<V>  {

	
	public V process(Row row);
	
}
