package reader;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

	@Override
	public void processResult(Object obj) {
		if (rowCount.putIfAbsent(obj, new AtomicInteger(1)) != null) {
			rowCount.get(obj).getAndIncrement();
		}
		
	}

}
