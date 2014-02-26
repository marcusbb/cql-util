package reader;

import static org.junit.Assert.*;

import org.junit.Test;

import reader.samples.RowCountJob;

import driver.em.CassConfig;

public class CountTest {

	public void testCountJob() {
		RowCountJob job = new RowCountJob();
		//set this appropriately
		CassConfig Cconfig = new CassConfig();
		ReaderConfig config = new ReaderConfig();
		config.setCassConfig(Cconfig);
		
		CQLRowReader reader = new CQLRowReader(config, job);
		
		reader.read();
		
	}
}
