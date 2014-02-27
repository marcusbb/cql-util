package reader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MTJobBootStrap extends JobBootStrap {

	protected ExecutorService executor;
	
	static class TPFactory implements ThreadFactory {
		AtomicInteger index = new AtomicInteger(1);
		
		@Override
		public Thread newThread(Runnable arg0) {
			Thread t = new Thread(arg0);
			t.setName("MTJOB_"+index.getAndIncrement());
			return t;
		}
		
	}
	int nThreads = -1;
	
	public MTJobBootStrap(int nThreads) {
		this.nThreads = nThreads;
		
		
		executor = Executors.newFixedThreadPool(nThreads);
		
		long delta = (config.getEndToken()/2 - config.getStartToken()/2)/nThreads *2;
		long next = config.getStartToken();
		
		config.setTokenRanges(new ReaderConfig.TokenRange[nThreads]);
		//create the token ranges: one per thread
		for (int i=0;i<nThreads;i++) {
			
			config.getTokenRanges()[i].setStartToken(next);
			if (i < (nThreads-1) )
				config.getTokenRanges()[i].setEndToken(next + delta -1);
			else
				config.getTokenRanges()[i].setEndToken(config.getEndToken());
			
			next = next + delta;
			
		}
		
	}

	/**
	 * Multi threaded execution: One thread per reader.
	 * 
	 */
	@Override
	public void runJob() {
		
		for (int i=0;i<nThreads;i++) {
			
			final ReaderConfig.TokenRange[] tokenRanges = config.getTokenRanges();
			final Integer index = i;
			executor.submit(new Runnable() {
				
				@Override
				public void run() {
					reader.read(tokenRanges[index].getStartToken(),tokenRanges[index].getEndToken());
					
				}
			});
		}
		executor.shutdown();
		job.onReadComplete();
		
		logger.info("Shutting down cluster");
		reader.cluster.shutdown();
		
	}
	
	
	

}
