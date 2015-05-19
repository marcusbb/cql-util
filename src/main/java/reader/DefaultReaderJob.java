package reader;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

public class DefaultReaderJob<V> extends MTJobBootStrap implements IReaderJob<V> {

	@Override
	public IReaderJob<V> initJob(ReaderConfig readerConfig) {
		return this;
	}

	@Override
	public RowReaderTask<V> newTask() throws Exception {
		return new RowReaderTask<V>() {

			@Override
			public V process(Row row, ColumnDefinitions colDef,
					ExecutionInfo execInfo) {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}

	/**
	 * Wraps bootstrap and run
	 */
	public void kickOff() {
		bootstrap();
		runJob();
	}
	@Override
	public void processResult(V result) {
		
		
	}

	@Override
	public void onReadComplete() {
		// TODO Auto-generated method stub
		
	}
	

}
