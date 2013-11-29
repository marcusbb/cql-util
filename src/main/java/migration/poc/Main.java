package migration.poc;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import driver.em.CUtils;
import driver.em.CassConfig;

public class Main {

	
	public static void main(String []args) throws Exception {
		
		Cluster cluster;
		String ks = "olympia";
		
		cluster = CUtils.createCluster(new CassConfig());
		Session session = CUtils.createSession(cluster, ks);
		
		JAXBContext jc = JAXBContext.newInstance(XMLConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.xml");
		
		XMLConfig config = (XMLConfig)unmarshaller.unmarshal(ins);
		
		
		RSExecutor executor = new RSExecutor(config, session);
		executor.execute();
		
		System.exit(0);
		
	}

}
