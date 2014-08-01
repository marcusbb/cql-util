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
	ColumnInfo colName;
	final Integer threshold;
	ConcurrentHashMap<Object,AtomicInteger> rowCount = new ConcurrentHashMap<>();
	private static Logger logger = LoggerFactory.getLogger(DistinctCountJob.class);
	
	public DistinctCountJob(Integer threshold) {
		
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

		//The column to form the count around
		ColumnInfo colName;
		final Integer threshold;
		DistinctCountJob job = null;
		public DistinctCountBatchJob(Integer threshold,int numThreads) {
			super(numThreads);
			//this.colName = colName;
			this.threshold = threshold;
		}
		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig) {
			
			return job;
		}
		
		public static void main(String []args) {
			String colName = args[0];
			int threshold = Integer.parseInt(args[1]);
			int threads = 1;
			if (args.length >= 2)
				threads = Integer.parseInt(args[2]);
			
			ColumnInfo colInfo = new ColumnInfo(colName, DataType.ascii());
			//think about this renaming some classes with reference to a batch job, vs job			
			DistinctCountBatchJob batchJob = new DistinctCountBatchJob( threshold,threads);
			batchJob.job = new DistinctCountJob(threshold);
			
			batchJob.bootstrap();
			//After boostrap we should now have the necessary configuration			
			ColumnInfo[] partKeys = batchJob.config.getPkConfig().getPartitionKeys();
			ColumnInfo[] clustKeys = batchJob.config.getPkConfig().getClusterKeys();
			for (ColumnInfo ci:partKeys) {
				if (ci.getName().equals(colName))
					colInfo = new ColumnInfo(colName, ci.getType());
			}
			for (ColumnInfo ci:clustKeys) {
				if (ci.getName().equals(colName))
					colInfo = new ColumnInfo(colName, ci.getType());
			}
				
			batchJob.job.colName = colInfo;
			
			batchJob.runJob();
			
		}
	}

	
	@Override
	public void onReadComplete() {
		
		logger.info("#########################################################");
		logger.info("#Summary##################################################");
		logger.info("#########################################################");
		logger.info("Total distinct {}={}",colName.getName(), rowCount.size() );
		for (Object rowObj:rowCount.keySet()) {
			AtomicInteger size = rowCount.get(rowObj);
			if (size.intValue() >= threshold) {
				logger.info("col[{}],size[{}]",rowObj,size.intValue());
			}
		}
		logger.info("#########################################################");
	}

	
}
