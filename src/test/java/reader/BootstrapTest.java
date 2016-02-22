package reader;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import driver.em.TestBase;
import reader.samples.RowCountJob;

@Ignore
public class BootstrapTest extends TestBase{

	
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
		
		MTJobBootStrap bootstrap = new MTJobBootStrap() {

			

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
