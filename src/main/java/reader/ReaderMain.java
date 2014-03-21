package reader;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;

import driver.em.CUtils;
import driver.em.CassConfig;
/**
 * 
 * @deprecated
 *
 */
public class ReaderMain {

	static Cluster cluster = null;
	static Logger logger = LoggerFactory.getLogger(ReaderMain.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		String configFile = System.getProperty("config");
		String startToken = System.getProperty("startToken");
		String endToken = System.getProperty("endToken");
		
		if (configFile == null)
			configFile = "reader-config.xml";
		
		CQLRowReader reader = new CQLRowReader();
		
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
		
		reader.config = (ReaderConfig)unmarshaller.unmarshal(ins);
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
}
