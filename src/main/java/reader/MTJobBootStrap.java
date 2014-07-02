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
	int nThreads = -1;
	
	public MTJobBootStrap(int nThreads) {
		this.nThreads = nThreads;
		
		executor = Executors.newFixedThreadPool(nThreads,new TPFactory());
		
	}

	
	private void splitRanges() {

		
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
		splitRanges();
		for (int i=0;i<nThreads;i++) {
			
			final ReaderConfig.TokenRange[] tokenRanges = config.getTokenRanges();
			final Integer index = i;
			executor.submit(new Runnable() {
				CQLRowReader reader = new CQLRowReader(config,job,cluster,cluster.connect(config.getKeyspace()));
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
		cluster.shutdown();
		//
		logger.info("Complete multi-threaded read, total rows processed: {}", totalRows);
		
	}
	
	
	

}
