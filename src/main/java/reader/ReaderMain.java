package reader;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import com.datastax.driver.core.Cluster;

import driver.em.CUtils;
import driver.em.CassConfig;

public class ReaderMain {

	static Cluster cluster = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		String configFile = System.getProperty("config");
		if (configFile == null)
			configFile = "reader-config.xml";
				
		CQLRowReader reader = new CQLRowReader();
		
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
		
		reader.config = (ReaderConfig)unmarshaller.unmarshal(ins);
		
		reader.cluster = CUtils.createCluster(reader.config.getCassConfig());
		reader.session = reader.cluster.connect(reader.config.getKeyspace());
		
		//for exception safety below
		//Class.forName( reader.config.getReaderTask() ).newInstance();
		
		reader.read();
		
		cluster.shutdown();
		
	}
}
