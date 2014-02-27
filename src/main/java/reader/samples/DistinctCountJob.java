package reader.samples;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import reader.CQLRowReader;
import reader.JobBootStrap;
import reader.PKConfig;
import reader.PKConfig.ColumnInfo;
import reader.ReaderConfig;
import reader.ReaderJob;
import reader.RowReaderTask;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

public class DistinctCountJob extends ReaderJob<Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1456068572420793553L;
	final ColumnInfo colName;
	final Integer threshold;
	ConcurrentHashMap<Object,AtomicInteger> rowCount = new ConcurrentHashMap<>();
	
	public DistinctCountJob(ColumnInfo colName,Integer threshold) {
		this.colName = colName;
		this.threshold = threshold;
	}
	@Override
	public RowReaderTask<Object> newTask() throws Exception {
		return new LargeRowsTask(colName,threshold);
	}

	public class LargeRowsTask implements RowReaderTask<Object>{

		
		final PKConfig.ColumnInfo colToCount;
		final int threshold;
		
		public LargeRowsTask(ColumnInfo colToCount,int threshold) {
			this.colToCount = colToCount;
			this.threshold = threshold;
		}
		@Override
		public Object process(Row row,ColumnDefinitions colDef,ExecutionInfo execInfo) {
			
			Object colObj = CQLRowReader.get(row, colToCount);//row.getString(colToCount);
			
			
			return colObj;
			
			
		}

	}

	@Override
	public void processResult(Object obj) {
		if (rowCount.putIfAbsent(obj, new AtomicInteger(1)) != null) {
			rowCount.get(obj).getAndIncrement();
		}
		
	}
	
	public static class DistinctCountBatchJob extends JobBootStrap {

		final ColumnInfo colName;
		final Integer threshold;
		public DistinctCountBatchJob(ColumnInfo colName,Integer threshold) {
			this.colName = colName;
			this.threshold = threshold;
		}
		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig) {
			return new DistinctCountJob(colName, threshold);
		}
		
	}

	@Override
	public void onReadComplete() {
		// TODO Auto-generated method stub
		
	}

}
