package migration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.JAXBUtil;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import driver.em.CUtils;

public class Main {
	//do this first to initialize jvm properties file into system properties
	static Logger logger;
	static{
		initializeJvmProperties();
		logger = LoggerFactory.getLogger(Main.class);
	}
	
	public String fileName = "config.xml";
	
	public Main(String mappingFileName){
		
		if(mappingFileName != null){
			this.fileName = mappingFileName;
		}
	}
	
	public void execute() throws Exception{
		
		XMLConfig config = (XMLConfig)JAXBUtil.unmarshalXmlFile(fileName, XMLConfig.class);
		Cluster cluster = null;
		
		Map<String, Session> sessions = new HashMap<String, Session>();
		
		try{
			
			cluster = CUtils.createCluster(config.getCassConfig());
			if(config.getKeyspace() != null){
				sessions.put(config.getKeyspace(), CUtils.createSession(cluster, config.getKeyspace()));
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
			
		}
		catch(Exception e){
			logger.error("Exception in Main: " + e.getMessage(), e);
		}finally{
		
			if(cluster != null){
				logger.info("Shutting down the Cluster");
				boolean isShutdown = cluster.shutdown(300000, TimeUnit.MILLISECONDS);
				logger.info("Cluster Shutdown: " + isShutdown);
			}
		}
	}
	
	static void initializeJvmProperties(){
		
		String jvmPropsFile =  System.getProperty("migration.jvm.propertiesFile");
		if(jvmPropsFile != null){
			
			System.out.println("Loading JVM properties File: " + jvmPropsFile);
			Properties systemProps = new Properties();
			try {
				systemProps.load(new FileInputStream(jvmPropsFile));
			} catch (FileNotFoundException e) {
				System.out.println("JVM properties File Not Found: " + jvmPropsFile);
				System.exit(1);
			} catch (IOException e) {
				System.out.println("IOException reading JVM properties: " + jvmPropsFile + "; Message: " + e.getMessage());
				e.printStackTrace(System.out);
				System.exit(1);
			}
			
			//using this replaces all environment variables so set one by one
			//System.setProperties(systemProps);
			if(!systemProps.isEmpty()){
				Set<Entry<Object, Object>> sysProps = systemProps.entrySet();
				for(Entry<Object, Object> sysProp: sysProps){
					System.setProperty((String)sysProp.getKey(), (String)sysProp.getValue());
				}
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
