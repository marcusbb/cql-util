package reader;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

public class Stubs {
	public static class MyReaderJob extends ReaderJob<Void> {

		@Override
		public RowReaderTask<Void> newTask() throws Exception {
			return new MyReaderTask();
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
	public static class MyReaderTask implements RowReaderTask<Void> {

		@Override
		public Void process(Row row, ColumnDefinitions colDef,
				ExecutionInfo execInfo) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

}
