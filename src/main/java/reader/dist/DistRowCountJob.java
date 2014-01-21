package reader.dist;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import reader.RowReaderTask;

import com.datastax.driver.core.Row;

public class DistRowCountJob extends DistReaderJob<Void> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8912817777747352961L;

	private final AtomicLong localCount = new AtomicLong();
	private final AtomicLong finalCount = new AtomicLong();
	
	public static class Callback extends CompletionCallback implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -3673045568538378299L;
		private final AtomicLong finalCount;
		
		public Callback(final AtomicLong finalCount) {
			this.finalCount = finalCount;
		}
		@Override
		public void onResponse(ReaderResult readerResult) {

			finalCount.getAndIncrement();
			
		}
		
		
	
		
		
	}
	//The CQL row tasks we're doing
	public static class RowTask implements RowReaderTask<Void> {
		final AtomicLong count;
		public RowTask(AtomicLong count) {
			this.count = count;
		}
		@Override
		public Void process(Row row) {
			count.incrementAndGet();
			return null;
		}
		
	}
	//The callback 
	//for count the ReaderResult already encapsulates it
	@Override
	public CompletionCallback newCompletionCallBack() {

		return new Callback(finalCount);
	}

	


	@Override
	public RowReaderTask<Void> newTask() throws Exception {
		return new RowTask(localCount);
	}




	@Override
	public void processResult(Void result) {
		// TODO Auto-generated method stub
		
	}

	

}
