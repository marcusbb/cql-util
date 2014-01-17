package reader.dist;

import java.util.concurrent.Callable;

import reader.CQLRowReader;
import reader.ReaderJob;


public class DistReadTask implements Callable<ReaderResult>{

	private DistReaderConfig readerConfig;
	
	private ReaderJob job;
	
	public DistReadTask(DistReaderConfig readerConfig,ReaderJob job) {
		this.readerConfig = readerConfig;
		this.job = job;
	}
	@Override
	public ReaderResult call() throws Exception {
		
		ReaderResult result = new ReaderResult();
		
		//the main show
		CQLRowReader reader = new CQLRowReader(readerConfig, job);
		reader.read();
		
				
		result.rowsRead = reader.getTotalReadCount();

		return result;
	}

	
}
