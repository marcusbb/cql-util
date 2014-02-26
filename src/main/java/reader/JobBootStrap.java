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

import com.datastax.driver.core.ColumnMetadata;

import driver.em.CUtils;

public abstract class JobBootStrap {

	static Logger logger = LoggerFactory.getLogger(JobBootStrap.class);
	
	protected CQLRowReader reader = null;
	protected ReaderConfig config;
	protected ReaderJob<?> job;
	
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
		job = initJob(config);
		CQLRowReader reader = new CQLRowReader(config,job);
		
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
			reader.config.setStartToken(Long.parseLong(startToken));
		if (endToken!=null)
			reader.config.setEndToken(Long.parseLong(endToken));
		reader.cluster = CUtils.createCluster(reader.config.getCassConfig());
		reader.session = reader.cluster.connect(reader.config.getKeyspace());

	}
	
	public void runJob() {

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
