package reader;

import static org.junit.Assert.*;

import org.junit.Test;

import reader.samples.RowCountJob;

public class BootstrapTest {

	@Test
	public void singlethreaded() {
		System.setProperty("config", "perf-reader-config.xml");
		
		final RowCountJob  rowCountJob = new RowCountJob();
		//int nThreads = 10;
		
		JobBootStrap bootstrap = new JobBootStrap() {

			@Override
			public ReaderJob<?> initJob(ReaderConfig readerConfig) {
				return rowCountJob;
			}
			
		};
		//first bootstrap
		bootstrap.bootstrap();
		
		//then run
		bootstrap.runJob();
	}
	
	@Test
	public void multithreaded() {
		System.setProperty("config", "perf-reader-config.xml");
		
		final RowCountJob  rowCountJob = new RowCountJob();
		int nThreads = 10;
		
		MTJobBootStrap bootstrap = new MTJobBootStrap(nThreads) {

			@Override
			public ReaderJob<?> initJob(ReaderConfig readerConfig) {
				return rowCountJob;
			}
			
		};
		//first bootstrap
		bootstrap.bootstrap();
		
		//then run
		bootstrap.runJob();
	}

}
