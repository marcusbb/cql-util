package reader;

import com.datastax.driver.core.Row;

public class LoggingRowTask implements RowReaderTask<Void> {

	@Override
	public void process(Row row) {
		//log something		
	}

	
}
