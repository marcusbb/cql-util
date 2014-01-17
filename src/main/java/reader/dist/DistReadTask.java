package reader.dist;

import java.util.concurrent.Callable;

import reader.CQLRowReader;


public class DistReadTask implements Callable<ReaderResult>{

	private DistReaderConfig readerConfig;
	
	public DistReadTask(DistReaderConfig readerConfig) {
		this.readerConfig = readerConfig;
	}
	@Override
	public ReaderResult call() throws Exception {
		
		ReaderResult result = new ReaderResult();
		
		//the main show
		CQLRowReader reader = new CQLRowReader(readerConfig);
		reader.read();
		
				
		result.rowsRead = reader.getTotalReadCount();

		return result;
	}

	
}
