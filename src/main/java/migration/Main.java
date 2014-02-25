package migration;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import driver.em.CUtils;

public class Main {
	static Logger logger =  LoggerFactory.getLogger(Main.class);
	public String fileName = "config.xml";
	
	public Main(String mappingFileName){
		if(mappingFileName != null){
			this.fileName = mappingFileName;
		}
	}
	
	public void execute() throws Exception{
		
		InputStream ins = null;
		File file = new File(fileName);
		if(file.exists()){
			ins = file.toURI().toURL().openStream();
		}else{
			ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
		}
		
		JAXBContext jc = JAXBContext.newInstance(XMLConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		XMLConfig config = (XMLConfig)unmarshaller.unmarshal(ins);
		
		Cluster cluster = CUtils.createCluster(config.getCassConfig());
		
		try{
			Map<String, Session> sessions = new HashMap<String, Session>();
			
			if(config.getKeyspace() != null){
				sessions.put(RSExecutor.DEFAULT_KEY, CUtils.createSession(cluster, config.getKeyspace()));
			}
			
			List<RSToCqlConfig> rsToCqlConfigs = config.getRsToCqlConfigs();
			for(RSToCqlConfig rsToCqlConfig : rsToCqlConfigs){
				String keyspace = rsToCqlConfig.getKeyspace();
				if(keyspace != null && !sessions.containsKey(keyspace)){
					sessions.put(keyspace, CUtils.createSession(cluster, keyspace));
				}
			}
			
			RSExecutor executor = new RSExecutor(config, sessions);
			executor.execute();
			
		}finally{
			if(cluster != null){
				logger.info("Shutting down the Cluster");
				boolean isShutdown = cluster.shutdown(300000, TimeUnit.MILLISECONDS);
				logger.info("Cluster Shutdown: " + isShutdown);
			}
		}
	}
	
	public static void main(String []args) throws Exception {
		
		String fileName = null;
	
		if(args != null && args.length == 1){
			fileName = args[0];
		}
		
		Main main = new Main(fileName);
		main.execute();
		
		System.exit(0);
	}
}
