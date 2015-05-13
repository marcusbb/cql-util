package reader;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reader.ReaderConfig.TokenRange;

public abstract class MTJobBootStrap extends JobBootStrap {

	protected ExecutorService executor;
	
	protected AtomicLong totalRows = new AtomicLong(0);
	private static Logger logger = LoggerFactory.getLogger(MTJobBootStrap.class);
	
	static class TPFactory implements ThreadFactory {
		AtomicInteger index = new AtomicInteger(1);
		
		@Override
		public Thread newThread(Runnable arg0) {
			Thread t = new Thread(arg0);
			t.setName("MTJOB_"+index.getAndIncrement());
			return t;
		}
		
	}
	
	public MTJobBootStrap() {}
	
	public MTJobBootStrap(ExecutorService execService) {
		this.executor = execService;
	}
	private void splitRanges() {

		if (config == null)
			throw new IllegalArgumentException("config hasn't been set, bootstrap hasn't been called");
		int nThreads = config.getNumThreads();
		
		long delta = (config.getEndToken()/2 - config.getStartToken()/2)/nThreads *2;
		long next = config.getStartToken();
		
		ArrayList<TokenRange> rangeList = new ArrayList<TokenRange>(nThreads);
		//create the token ranges: one per thread
		for (int i=0;i<nThreads;i++) {
			TokenRange range = new TokenRange(next, config.getEndToken());
			
			if (i < (nThreads-1) )
				range.setEndToken(next + delta -1);

			rangeList.add(range);
			
			next = next + delta;
			
		}
		config.setTokenRanges(rangeList.toArray(new TokenRange[0]));
		logger.info("Split token ranges {}",rangeList);
	}

	/**
	 * Multi threaded execution: One thread per reader.
	 * 
	 */
	@Override
	public void runJob() {
		if (!initialized)
			throw new IllegalArgumentException("Uninitialized bootstrap");
		if (config == null)
			throw new IllegalArgumentException("config hasn't been set, bootstrap hasn't been called");
		int nThreads = config.getNumThreads();
		if (executor == null) {
			logger.info("Using internal thread pool with threads {}", nThreads);
			executor = Executors.newFixedThreadPool(nThreads);
		}
		splitRanges();
		
		for (int i=0;i<nThreads;i++) {
			
			final ReaderConfig.TokenRange[] tokenRanges = config.getTokenRanges();
			final Integer index = i;
			executor.submit(new Runnable() {
				CQLRowReader reader = new CQLRowReader(config,job,cluster,getSession());
				@Override
				public void run() {
					reader.read(tokenRanges[index].getStartToken(),tokenRanges[index].getEndToken());
					
					totalRows.addAndGet(reader.getTotalReadCount());
				}
			});
		}
		try {
			executor.shutdown();
			executor.awaitTermination(30, TimeUnit.DAYS);
		}catch (InterruptedException e) {
			e.printStackTrace();
		}
		job.onReadComplete();
		
		logger.info("Shutting down cluster");
		cluster.close();
		//
		logger.info("Complete multi-threaded read, total rows processed: {}", totalRows);
		
	}
	
	
	

}
