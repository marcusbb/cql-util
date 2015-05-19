package reader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reader.PKConfig.ColumnInfo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import driver.em.CUtils;
import driver.em.CassConfig;


/**
 * Bootstrap is a once only operation, like a builder operation
 * 
 *  but also connects to the cluster and discovers meta data\
 *  
 */

public abstract class JobBootStrap {

	static Logger logger = LoggerFactory.getLogger(JobBootStrap.class);
	
	//protected CQLRowReader reader = null;
	protected ReaderConfig config;
	protected IReaderJob<?> job;
	protected Cluster cluster;
	protected Session session;
	protected volatile boolean initialized = false;
	protected final static String DEF_CONFIG = "reader-config.xml";
	
	/**
	 * Bootstrap with a given {@link InputStream}
	 * @param configClass - by default this is {@link ReaderConfig} class
	 * @param stream
	 * @throws JAXBException 
	 * @throws ClassNotFoundException 
	 */
	public void bootstrap(String configClass, InputStream stream) throws ClassNotFoundException, JAXBException  {
		String c = ReaderConfig.class.getName();
		if (configClass == null)
			c = ReaderConfig.class.getName();		
		JAXBContext jc = JAXBContext.newInstance(Class.forName(c));
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		this.config = (ReaderConfig)unmarshaller.unmarshal(stream);
		doBoostrap(this.config);
	}
	/**
	 * Bootstrap with a provided xml string, calling 
	 * {@link #bootstrap(String, InputStream)}
	 * 
	 * @param xml
	 * @throws ClassNotFoundException
	 * @throws JAXBException
	 */
	public void boostrap(String xml) throws ClassNotFoundException, JAXBException  {
		ByteArrayInputStream bin = new ByteArrayInputStream(xml.getBytes());
		bootstrap(ReaderConfig.class.getName(),bin);
	}
	/**
	 * 
	 * @param config
	 */
	public void bootstrap(ReaderConfig config) {
		doBoostrap(config);
		this.config = config;
	}
	/**
	 * bootstrap this job with a given cluster and session (managed elsewhere)
	 * {@link CassConfig} from {@link ReaderConfig} is ignored
	 * in this bootstrap mechanism.
	 *  
	 * @param cluster
	 * @param session
	 * @param config
	 */
	public void bootstrap(Cluster cluster,Session session, ReaderConfig config) {
		this.job = initJob(config);
		this.config = config;
		this.cluster = cluster;
		if (session == null)
			this.session = cluster.connect(config.getKeyspace());
		validate(config,true); 
		discover(config);
		initialized = true;
	}
	
	/**
	 * 
	 */
	protected void bootstrap() {
		String configFile = System.getProperty("config");
		String startToken = System.getProperty("startToken");
		String endToken = System.getProperty("endToken");
		String configClass = System.getProperty("configClass");
		if (configFile == null)
			configFile = DEF_CONFIG;
		if (configClass == null)
			configClass = ReaderConfig.class.getName();
		
		try {
			JAXBContext jc = JAXBContext.newInstance(Class.forName(configClass));
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
			if (ins == null)
				throw new IllegalArgumentException("File from classpath: " + configFile + " not found");
			config = (ReaderConfig)unmarshaller.unmarshal(ins);
			//something is not quite right here
			//Marshaller marshaller = jc.createMarshaller();
			//System.out.print("Config: ");
			//marshaller.marshal(config, System.out);
			//discover the pk information:
			
		}catch (ClassNotFoundException e) {
			logger.error(e.getMessage(),e);
			logger.error("Unrecoverable error Reader config class {} is unrecognizable " + configClass);
			System.exit(1);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			logger.error("Unrecoverable error reading configuration, please make sure reader-config.xml is valid and readable");
			System.exit(1);
		} 
		
		
		if (startToken!=null)
			config.setStartToken(Long.parseLong(startToken));
		if (endToken!=null)
			config.setEndToken(Long.parseLong(endToken));
		
		doBoostrap(this.config);
		

	}
	
	/**
	 * Create connection to the cluster, discover meta data
	 * and validate
	 * 
	 * @param config
	 */
	private void doBoostrap(ReaderConfig config) {
		this.cluster = CUtils.createCluster(config.getCassConfig());
		this.session = cluster.connect(config.getKeyspace());
		this.job = initJob(config);
		validate(config,false);
		discover(config);
		initialized = true;
		
	}
	/**
	 * Much of the validation can be caught in an xslt if one is to be provided
	 */
	private void validate(ReaderConfig config,boolean allowNullCassConfig) {
		if (config == null)
			throw new IllegalArgumentException("ReaderConfig must not be null");
		if (!allowNullCassConfig && config.getCassConfig() == null)
			throw new IllegalArgumentException("ReaderConfig.cassconfig cannot be null ");
		if (config.getKeyspace() == null)
			throw new IllegalArgumentException("ReaderConfig.keyspace cannot be null ");
		if (config.getEndToken() < config.getStartToken())
			throw new IllegalArgumentException("End Token must be larger than start token");
		if (config.getTable() == null)
			throw new IllegalArgumentException("ReaderConfig.table cannot be null");
			
	}
	private void discover(ReaderConfig config) {
		Session s = cluster.connect("system");
		//s.execute("select * from schema_columns");
		//s.execute("select * from schema_columns");
		
		//Not sure the dynamic configuration works yet
		TableMetadata tbm = cluster.getMetadata().getKeyspace(config.getKeyspace()).getTable(config.getTable());
		if (tbm == null)
			throw new IllegalArgumentException("Readerconfig.table is not available");
		List<ColumnMetadata> colMeta = tbm.getPartitionKey();
		
		List<ColumnMetadata> colClusMeta = cluster.getMetadata().getKeyspace(config.getKeyspace()).getTable(config.getTable()).getClusteringColumns();
		
		ColumnInfo []partitionCols = new ColumnInfo[colMeta.size()];
		ColumnInfo []clusterCols = new ColumnInfo[colClusMeta.size()];
		
		int i = 0;
		for (ColumnMetadata col:colMeta) {
			partitionCols[i++] = new ColumnInfo(col);
		}
		i = 0;
		for (ColumnMetadata col:colClusMeta) {
			clusterCols[i++] = new ColumnInfo(col);
		}
		
		config.getPkConfig().setPartitionKeys(partitionCols);
		config.getPkConfig().setClusterKeys(clusterCols);
		
		
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
		reader.cluster.close();
	}
	
	/**
	 * Give me your job
	 * @return
	 */
	public abstract IReaderJob<?> initJob(final ReaderConfig readerConfig);
	
	
	public ReaderConfig getConfig() {
		return config;
	}
	public Cluster getCluster() {
		return cluster;
	}
	public Session getSession() {
		return session;
	}
	
	

}
