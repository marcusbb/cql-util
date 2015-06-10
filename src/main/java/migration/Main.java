package migration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.JAXBUtil;

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
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try{
			RSExecutor executor = new RSExecutor(config);
			mbs.registerMBean(executor, new ObjectName("cqlutil:type=rsexecutor"));
			executor.execute();
		}
		catch(Exception e){
			logger.error("Exception in Main: " + e.getMessage(), e);
		}finally{
		
			
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
		
		new Main(fileName).execute();
		
		System.exit(0);
	}
}
