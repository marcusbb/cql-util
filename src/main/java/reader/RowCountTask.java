package reader;

import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.Row;

public class RowCountTask implements RowReaderTask<Void>{

	
	static AtomicLong count  = new AtomicLong(0);
	
	@Override
	public void process(Row row) {
		count.incrementAndGet();
		
	}

	

}
