package reader;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import driver.em.CUtils;

public abstract class JobBootStrap {

	static Logger logger = LoggerFactory.getLogger(JobBootStrap.class);
	
	protected CQLRowReader reader = null;
	protected ReaderConfig config;
	
	/**
	 * Should only be called once, can add checking but 
	 * developers should use common sense.
	 */
	protected void bootstrap() {
		String configFile = System.getProperty("config");
		String startToken = System.getProperty("startToken");
		String endToken = System.getProperty("endToken");
		String configClass = System.getProperty("configClass");
		if (configFile == null)
			configFile = "reader-config.xml";
		if (configClass == null)
			configClass = ReaderConfig.class.getName();
		
		try {
			JAXBContext jc = JAXBContext.newInstance(Class.forName(configClass));
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
			
			config = (ReaderConfig)unmarshaller.unmarshal(ins);
		}catch (ClassNotFoundException e) {
			logger.error(e.getMessage(),e);
			logger.error("Unrecoverable error Reader config class {} is unrecognizable " + configClass);
			System.exit(1);
		} catch (JAXBException e) {
			logger.error(e.getMessage(),e);
			logger.error("Unrecoverable error reading configuration, please make sure reader-config.xml is valid and readable");
			System.exit(1);
		} 
		CQLRowReader reader = new CQLRowReader(config,initJob(config));
		if (startToken!=null)
			reader.config.setStartToken(Long.parseLong(startToken));
		if (endToken!=null)
			reader.config.setEndToken(Long.parseLong(endToken));
		reader.cluster = CUtils.createCluster(reader.config.getCassConfig());
		reader.session = reader.cluster.connect(reader.config.getKeyspace());

		try {
			reader.read();
		}catch (Exception e) {
			logger.error(e.getMessage(),e);
		}

		
		//hack util we have proper reporting and job handling
		/*RowReaderTask<?> rowReader = (RowReaderTask<?>)Class.forName( reader.config.getReaderTask() ).newInstance();
		if (rowReader instanceof LargeRowsTask) {
			LargeRowsTask task = (LargeRowsTask)rowReader;
			task.printAll();
		}*/
		//end hack
		logger.info("Shutting down cluster");
		reader.cluster.shutdown();
	}
	
	
	/**
	 * Give me your job
	 * @return
	 */
	public abstract ReaderJob<?> initJob(final ReaderConfig readerConfig);

}
