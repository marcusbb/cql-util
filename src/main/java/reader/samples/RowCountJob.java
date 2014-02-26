package reader.samples;

import java.util.concurrent.atomic.AtomicLong;

import reader.ReaderJob;
import reader.RowReaderTask;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

public class RowCountJob extends ReaderJob<Void> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6758590887882801044L;
	final AtomicLong count = new AtomicLong();
	
	public static class RowTask implements RowReaderTask<Void> {
		final AtomicLong count;
		public RowTask(AtomicLong count) {
			this.count = count;
		}
		@Override
		public Void process(Row row,ColumnDefinitions meta, ExecutionInfo execInfo) {
			count.incrementAndGet();
			return null;
		}
		
	}
	
	@Override
	public RowReaderTask<Void> newTask() throws Exception {
		return new RowTask(this.count);
	}

	@Override
	public void processResult(Void result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onReadComplete() {
		// TODO Auto-generated method stub
		
	}

	
	
}
