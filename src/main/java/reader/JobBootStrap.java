package reader;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Executor;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reader.PKConfig.ColumnInfo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;

import driver.em.CUtils;

public abstract class JobBootStrap {

	static Logger logger = LoggerFactory.getLogger(JobBootStrap.class);
	
	//protected CQLRowReader reader = null;
	protected ReaderConfig config;
	protected ReaderJob<?> job;
	protected Cluster cluster;
	protected volatile boolean initialized = false;
	
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
			if (ins == null)
				throw new IllegalArgumentException("File from classpath: " + configFile + " not found");
			config = (ReaderConfig)unmarshaller.unmarshal(ins);
			//discover the pk information:
			
		}catch (ClassNotFoundException e) {
			logger.error(e.getMessage(),e);
			logger.error("Unrecoverable error Reader config class {} is unrecognizable " + configClass);
			System.exit(1);
		} catch (JAXBException e) {
			logger.error(e.getMessage(),e);
			logger.error("Unrecoverable error reading configuration, please make sure reader-config.xml is valid and readable");
			System.exit(1);
		} 
		this.cluster = CUtils.createCluster(config.getCassConfig());
		job = initJob(config);
		
				
		//Not sure the dynamic configuration works yet
		/*List<ColumnMetadata> colMeta = reader.cluster.getMetadata().getKeyspace(config.getKeyspace()).getTable(config.getTable()).getPartitionKey();
		List<ColumnMetadata> colClusMeta = reader.cluster.getMetadata().getKeyspace(config.getKeyspace()).getTable(config.getTable()).getClusteringKey();
		
		ColumnInfo []partitionCols = new ColumnInfo[colMeta.size()];
		ColumnInfo []clusterCols = new ColumnInfo[colMeta.size()];
		
		int i = 0;
		for (ColumnMetadata col:colMeta) {
			partitionCols[i++] = new ColumnInfo(col);
		}
		i = 0;
		for (ColumnMetadata col:colClusMeta) {
			clusterCols[i++] = new ColumnInfo(col);
		}
		config.getPkConfig().setPartitionKeys(partitionCols);
		config.getPkConfig().setClusterKeys(clusterCols);*/
		
		if (startToken!=null)
			config.setStartToken(Long.parseLong(startToken));
		if (endToken!=null)
			config.setEndToken(Long.parseLong(endToken));
		
		initialized = true;

	}
	
	public void runJob() {
		CQLRowReader reader = new CQLRowReader(config, job, cluster, cluster.connect(config.getKeyspace()));
		if (!initialized)
			throw new IllegalArgumentException("Uninitialized bootstrap");
		try {
			reader.read();
		}catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		
		job.onReadComplete();
		
		logger.info("Shutting down cluster");
		reader.cluster.shutdown();
	}
	
	/**
	 * Give me your job
	 * @return
	 */
	public abstract ReaderJob<?> initJob(final ReaderConfig readerConfig);

}
