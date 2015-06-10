package migration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.shaded.netty.util.Timeout;
import com.datastax.shaded.netty.util.TimerTask;

import driver.em.CUtils;


public class RSExecutor implements RSExecutorMBean {
	
	private static String KEYSPACE_LOGGER_PREFIX = "cql.keyspace.";
	private static Logger logger =  LoggerFactory.getLogger(RSExecutor.class);
	
	private static long defaultKeepAlive = 60; 
	private static TimeUnit defaultKeepAliveTU = TimeUnit.SECONDS;
	private static int defaultQueueCapacity = 1000;
	private static int corePoolSize = 5;
	private static int maxPoolSize = 10;
	
	protected String[] keyspaces;
	protected Map<String, Session> sessions;
	protected Cluster cluster;
	
	protected Connection conn = null;
	protected Statement stmt = null;
	protected ResultSet rs =  null;
	
	protected Map<String, Logger> keyspaceLoggers;
	protected Map<String, List<com.datastax.driver.core.RegularStatement>> keyspaceBatchStatements;
	protected ThreadPoolExecutor exec;
	
	private ExecutorState state = ExecutorState.INIT;
	private CountDownLatch latch = null;
	
	
	XMLConfig config;
	long requestCount;
	long cqlRequestCount;
	AtomicLong asyncResultsCount = new AtomicLong(0l);
	
	public RSExecutor(XMLConfig config) throws ClassNotFoundException, SQLException{
		this.config = config;
		keyspaces = retrieveKeyspaces();
		
		exec = new  ThreadPoolExecutor(corePoolSize, maxPoolSize, defaultKeepAlive,  defaultKeepAliveTU,  new ArrayBlockingQueue<Runnable>(defaultQueueCapacity));
		if(config.getConsistencyLevel()==null){
			config.setConsistencyLevel(ConsistencyLevel.ONE);
		}
		keyspaceBatchStatements = new HashMap<String, List<com.datastax.driver.core.RegularStatement>>();
		keyspaceLoggers = new HashMap<String, Logger>();
		sessions = new HashMap<String, Session>();
		for(String keyspaceName : keyspaces){
			if(!keyspaceLoggers.containsKey(keyspaceName)){
				keyspaceLoggers.put(keyspaceName,  LoggerFactory.getLogger(KEYSPACE_LOGGER_PREFIX.concat(keyspaceName)));
			}
			if(!keyspaceBatchStatements.containsKey(keyspaceName)){
				keyspaceBatchStatements.put(keyspaceName, new ArrayList<com.datastax.driver.core.RegularStatement>(config.batchWrites));
			}
		}
	}
	
	private String[] retrieveKeyspaces(){
		List<String> keyspaces = new ArrayList<String>();
		
		if(config.getKeyspace() != null){
			keyspaces.add(config.getKeyspace());
		}
		
		List<RSToCqlConfig> rsToCqlConfigs = config.getRsToCqlConfigs();
		for(RSToCqlConfig rsToCqlConfig : rsToCqlConfigs){
			String keyspace = rsToCqlConfig.getKeyspace();
			if(keyspace != null && !keyspaces.contains(keyspace)){
				keyspaces.add(keyspace);
			}
		}
		
		return keyspaces.toArray(new String[keyspaces.size()]);
	}
	
	private void connectToKeyspaces(){
		cluster = CUtils.createCluster(config.getCassConfig());
		
		for(String keyspace : keyspaces){
			sessions.put(keyspace, CUtils.createSession(cluster, keyspace));
		}
	}
	
	private Connection getJdbcConnection() throws SQLException, ClassNotFoundException{
		if(config.getJdbcDriver() != null){
			Class.forName(config.getJdbcDriver());
		}
		
		return DriverManager.getConnection( config.getJdbcUrl(), config.getJdbcUsername(), config.getJdbcPassword());
	}
	
	List<RowToCql> getOperationStatements(ResultSet rs){
		List<RowToCql> operationStatements = new ArrayList<>();
		//build the Row and RowtoMap
		for (RSToCqlConfig cqlConfig:config.rsToCqlConfigs) {
			RowToCql rowToCql = null;
			if (cqlConfig.getNameMapping() != null)
				rowToCql = new RowToCqlMap(rs, cqlConfig.getCqlTable(), cqlConfig.getColumns().toArray(new JdbcColMapping[]{}),cqlConfig.getNameMapping(),cqlConfig.getValueMapping(), cqlConfig.getKeyspace());
			else
				rowToCql = new RowToCql(rs, cqlConfig.getCqlTable(), cqlConfig.getColumns().toArray(new JdbcColMapping[]{}), cqlConfig.getKeyspace() );
			operationStatements.add(rowToCql);
		}
		return operationStatements;
	}
	
	public void execute() throws SQLException, ClassNotFoundException,InterruptedException {
		
		requestCount = 0l;
		cqlRequestCount = 0l;
		asyncResultsCount.set(0l);
		
		logger.info("Configuration: " + config.toString());
		
		try{
			connectToKeyspaces();
			conn = getJdbcConnection();
			logger.info("Starting SQL execution: " + config.getSqlQuery() );
			stmt = conn.createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			long startExec = System.currentTimeMillis();
			state = ExecutorState.SQL_EXECUTION;
			rs = stmt.executeQuery(config.getSqlQuery());
			long endExec = System.currentTimeMillis();
			long elapsed = endExec-startExec;

			logger.info("Completed SQL execution: " + config.getSqlQuery() + " in (MS)" +elapsed );

			//build the Row and RowtoMap
			List<RowToCql> operationStatements = getOperationStatements(rs);
			state = ExecutorState.RUNNING;
						
			
			run(operationStatements);
			
			
			cqlRequestCount = (requestCount * operationStatements.size());
			processRemainingStatements();
			
			logger.info("completed: " + requestCount);
			
		}finally{
			cleanup();
		}
	}
	
	private void run(List<RowToCql> operationStatements) throws SQLException,InterruptedException {
		while (rs.next() ) {
			
			if (requestCount++ % 1000 == 0){
				logger.info("completed db rows: " + requestCount);
			}

			for (RowToCql row: operationStatements) {
				performCassandraExecution(row);
			}
			if (state == ExecutorState.SUSPENDED) {
				latch.await();
			}
		}
		
	}
 	public void cleanup(){
		if(config.asyncWrites && !config.bypassCassandra){
			
			while(asyncResultsCount.get() < cqlRequestCount){
				long processed = asyncResultsCount.get();
				logger.info("Asynchronous tasks still processing, main thread sleeping for 60 seconds." 
						+ " QueueSize: " + exec.getQueue().size() 
						+ "; Active Thread Count: " + exec.getActiveCount() 
						+ "; CQLRequestCount: " + cqlRequestCount
						+ "; ResultCount: " + processed
						+ "; Remaining Result Count: " + ( cqlRequestCount - processed ) );
				
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
				}
			}	
		}
		
		cleanupDBResources();
		cleanupCassandraResources();
	}
	
	protected void performCassandraExecution(RowToCql row) throws SQLException{
		
		final String keyspace = row.getKeyspace() != null?row.getKeyspace():config.getKeyspace();
		
		com.datastax.driver.core.RegularStatement statement = row.getStatement();
		try{
			boolean performBatchWrites = (config.batchWrites > 1);
			boolean writeToCassandra = !performBatchWrites;
			int batchSize = 1;
			
			List<com.datastax.driver.core.RegularStatement> statementList = keyspaceBatchStatements.get(keyspace);
			if(performBatchWrites){
				statementList.add(statement);
				
				batchSize = statementList.size();
				if(batchSize == config.batchWrites){	
					statement = QueryBuilder.batch(statementList.toArray(new com.datastax.driver.core.RegularStatement[batchSize]));
					statementList.clear();
					writeToCassandra = true;
				}
			}
			
			if(writeToCassandra){
				writeToCassandra(keyspace, statement, batchSize);
			}
		}catch (Exception e) {
			logger.error("Exception writing to cassandra: " + e.getMessage() + "; keyspace: " + keyspace, e);
			logFailedCql(keyspace, statement.getQueryString());
		}
	}

	protected void writeToCassandra(final String keyspace, final com.datastax.driver.core.RegularStatement statement, final int batchSize){
		if(config.bypassCassandra){
			return;
		}
	
		statement.setConsistencyLevel(config.getConsistencyLevel());
		
		Session session = sessions.get(keyspace);
		if(config.asyncWrites){
			final String queryString = statement.getQueryString();
			final ResultSetFuture future = session.executeAsync(statement);
			future.addListener( new Runnable(){

				@Override
				public void run() {
					try {
						future.getUninterruptibly();
					} catch (Throwable e) {
						logger.error("Exception: " + e.getMessage() + "; keyspace: " + keyspace, e);
						logFailedCql(keyspace, queryString);
					} 
					finally{
						asyncResultsCount.addAndGet(batchSize);
					}
				}

			}, exec);
		}else{
			session.execute(statement);
		}
	}
	
	protected void processRemainingStatements() {
		if(config.batchWrites > 1){
			for(String keyspace : keyspaceBatchStatements.keySet()){
				List<com.datastax.driver.core.RegularStatement> statementList = keyspaceBatchStatements.get(keyspace);
				int batchSize = statementList.size();
				if(batchSize > 0){
					com.datastax.driver.core.RegularStatement statement = QueryBuilder.unloggedBatch(statementList.toArray(new com.datastax.driver.core.RegularStatement[batchSize]));
					statementList.clear();
				
					writeToCassandra(keyspace, statement, batchSize);
				}
			}
		}
	}
	
	protected void logFailedCql(String keyspaceName, String queryString){
		queryString = queryString.trim();
		
		if(config.getBatchWrites() > 1){
			queryString = StringUtils.replace(queryString, "INSERT INTO", System.lineSeparator().concat("INSERT INTO"));
			queryString = StringUtils.replace(queryString, "APPLY BATCH", System.lineSeparator().concat("APPLY BATCH"));
			//syntax in batch statements cannot be applied via cqlsh as it is applied by cqldriver
			BufferedReader reader = new BufferedReader(new StringReader(queryString));
			
			StringBuilder builder = new StringBuilder();
			String line = null;
			try {
				while(( line = reader.readLine()) != null){
					builder.append(StringUtils.removeEnd(line.trim(), ";"));
					builder.append(System.lineSeparator());
				}
			} catch (IOException e) {
			}
			//remove last line separator character.
			builder.deleteCharAt(builder.length()-1);
			builder.append(";");
			queryString =  builder.toString();
		}else if(!queryString.endsWith(";")){
			queryString = queryString.concat(";");	
		}

		keyspaceLoggers.get(keyspaceName).error(queryString);
	}
	
	public long getRequestCount() {
		return requestCount;
	}

	public AtomicLong getAsyncResultsCount() {
		return asyncResultsCount;
	}

	private void cleanupDBResources(){
		close(rs);
		close(stmt);
		close(conn);
	}
	
	private void cleanupCassandraResources(){
		if(cluster != null){
			logger.info("Shutting down the Cluster");
			try {
			cluster.closeAsync().get(3000, TimeUnit.SECONDS);
			
			logger.info("Cluster Shutdown: " + cluster.isClosed());
			}catch (Exception e) {
				logger.error(e.getMessage(),e);
			}
		}
	}
	
	protected void close(ResultSet rs){
		if(rs != null){
			try {
				rs.close();
			} catch (SQLException e) {
				logger.error("Exception closing resultset: " + e.getMessage(), e);
			}
		}
	}
	
	protected void close(Statement stmt){
		if(stmt != null){
			try {
				stmt.close();
			} catch (SQLException e) {
				logger.error("Exception closing statement: " + e.getMessage(), e);
			}
		}
	}
	
	protected void close(Connection connection){
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error("Exception closing connection: " + e.getMessage(), e);
			}
		}
	}
	
	public ExecutorState getExecutorState() {
		return state;
	}

	@Override
	public synchronized void suspend() {
		if (state == ExecutorState.RUNNING) {
			latch = new CountDownLatch(1);
			state = ExecutorState.SUSPENDED;
		} else
			throw new IllegalStateException("Can only suspend from RUNNING state");
	}

	
	@Override
	public synchronized void suspendForMins(int mins) {
		if (state == ExecutorState.RUNNING) {
			latch = new CountDownLatch(1);
			state = ExecutorState.SUSPENDED;
			new Timer().schedule(new java.util.TimerTask() {
				
				@Override
				public void run() {
					resume();					
				}
			}, TimeUnit.MINUTES.toMillis(mins));
		}else {
			throw new IllegalStateException("Can only suspend from RUNNING state");
		}
		
	}

	@Override
	public synchronized void resume() {
		if (state == ExecutorState.SUSPENDED && latch != null) {
			
			state = ExecutorState.RUNNING;
			latch.countDown();
		}else {
			throw new IllegalStateException("Can only resume from SUSPENDED state");
		}
			
	}

	@Override
	public String getStateAsString() {
		return getExecutorState().toString();
	}
	
	
	
}
