package reader;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

//going to dump/refactor this class
public class LargeRowsTask implements RowReaderTask<Void>{

	//This may grow quite large
	private static ConcurrentHashMap<String,AtomicInteger> rowCount = new ConcurrentHashMap<>();
	static Logger logger = LoggerFactory.getLogger(LargeRowsTask.class);
	
	@Override
	public void process(Row row) {
		//hard-coded for now until we refactor to "job"
		String devId = row.getString("device_id");
		
		if (rowCount.putIfAbsent(devId, new AtomicInteger(1)) != null) {
			rowCount.get(devId).getAndIncrement();
		}
		
		
		
	}

	public static void printAll() {
		
		for (String key:rowCount.keySet()) {
			logger.info("dev[{}]: {}", key,rowCount.get(key));
		}
		//large rows
		
		for (String key:rowCount.keySet()) {
			if (rowCount.get(key).intValue() > 100)
				logger.info("largerow[{}]: {}", key,rowCount.get(key));
		}
		
	}
	
	

}
