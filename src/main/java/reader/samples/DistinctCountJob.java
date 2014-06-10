package reader.samples;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reader.CQLRowReader;
import reader.MTJobBootStrap;
import reader.PKConfig;
import reader.PKConfig.ColumnInfo;
import reader.ReaderConfig;
import reader.ReaderJob;
import reader.RowReaderTask;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
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
	private static Logger logger = LoggerFactory.getLogger(DistinctCountJob.class);
	
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

	//TODO: bug that is not calling
	@Override
	public void processResult(Object colObj) {

		if (rowCount.putIfAbsent(colObj, new AtomicInteger(1)) != null) {
			rowCount.get(colObj).getAndIncrement();
		}
	}
	
	public static class DistinctCountBatchJob extends MTJobBootStrap {

		final ColumnInfo colName;
		final Integer threshold;
		public DistinctCountBatchJob(ColumnInfo colName,Integer threshold,int numThreads) {
			super(numThreads);
			this.colName = colName;
			this.threshold = threshold;
		}
		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig) {
			return new DistinctCountJob(colName, threshold);
		}
		
		public static void main(String []args) {
			String colName = args[0];
			int threshold = Integer.parseInt(args[1]);
			int threads = 1;
			if (args.length >= 2)
				threads = Integer.parseInt(args[2]);
			
			ColumnInfo colInfo = new ColumnInfo(colName, DataType.ascii());
			//hack for now, until we think properly providing the job configuration
			if (colName.equals("column1"))
				colInfo = new ColumnInfo("column1", DataType.bigint());
			DistinctCountBatchJob job = new DistinctCountBatchJob(colInfo, threshold,threads);
			
			job.bootstrap();
			
			//job.config.getPkConfig().getPartitionKeys()
			
			job.runJob();
			
		}
	}

	
	@Override
	public void onReadComplete() {
		
		logger.info("#########################################################");
		logger.info("#Summary##################################################");
		logger.info("#########################################################");
		for (Object rowObj:rowCount.keySet()) {
			AtomicInteger size = rowCount.get(rowObj);
			if (size.intValue() >= threshold) {
				logger.info("col[{}],size[{}]",rowObj,size.intValue());
			}
		}
		logger.info("#########################################################");
	}

	
}
