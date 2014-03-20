package migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import driver.em.SimpleStatement;

public class RSExecutor {
   
	protected Map<String, Session> sessions;
	protected Map<String, Logger> keyspaceLoggers;
	
	static String KEYSPACE_LOGGER_PREFIX = "cql.keyspace.";
	static Logger logger =  LoggerFactory.getLogger(RSExecutor.class);
	XMLConfig config;
	long requestCount;
	AtomicLong asyncResultsCount = new AtomicLong(0l);
	
	private static long defaultKeepAlive = 60; 
	private static TimeUnit defaultKeepAliveTU = TimeUnit.SECONDS;
	private static int defaultQueueCapacity = 1000;
	private static int corePoolSize = 5;
	private static int maxPoolSize = 10;
	
	public RSExecutor(XMLConfig config, Session session) {
		HashMap<String, Session> sessions = new HashMap<String, Session>();
		sessions.put(config.getKeyspace(), session);
		
		init(config, sessions);
	}
	
	public RSExecutor(XMLConfig config, Map<String, Session> sessions) {
		init(config, sessions);
	}
	
	void init(XMLConfig config, Map<String, Session> sessions){
		this.config = config;
		this.sessions = sessions;
		addKeyspaceLogger(this.sessions.keySet().toArray(new String[this.sessions.size()]));
	}
	
	void addKeyspaceLogger(String... keyspaceNames){
		
		if(keyspaceLoggers == null){
			keyspaceLoggers = new HashMap<String, Logger>();
		}
		
		for(String keyspaceName : keyspaceNames){
			if(!keyspaceLoggers.containsKey(keyspaceName)){
				keyspaceLoggers.put(keyspaceName,  LoggerFactory.getLogger(KEYSPACE_LOGGER_PREFIX.concat(keyspaceName)));
			}
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
	
	public void execute() throws SQLException, ClassNotFoundException {
		requestCount = 0l;
		asyncResultsCount.set(0l);
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs =  null;
		
		try{
			conn = getJdbcConnection();
			
			stmt = conn.createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			long startExec = System.currentTimeMillis();
			rs = stmt.executeQuery(config.getSqlQuery());
			long endExec = System.currentTimeMillis();
			long elapsed = endExec-startExec;

			logger.info("Completed SQL execution: " + config.getSqlQuery() + " in (MS)" +elapsed );

			//build the Row and RowtoMap
			List<RowToCql> operationStatements = getOperationStatements(rs);
			
			ThreadPoolExecutor exec = new  ThreadPoolExecutor(corePoolSize, maxPoolSize, defaultKeepAlive,  defaultKeepAliveTU,  new ArrayBlockingQueue<Runnable>(defaultQueueCapacity));
			while (rs.next()) {
					if (requestCount++ % 1000 == 0){
						logger.info("completed db rows: " + requestCount);
					}

					for (RowToCql row: operationStatements) {
						final String keyspace = row.getKeyspace() != null?row.getKeyspace():config.getKeyspace();
						Session session = sessions.get(keyspace);
						final String queryString = row.getStatement().buildQueryString();
						
						try{
							if(config.asyncWrites){
								final ResultSetFuture future = session.executeAsync(queryString);
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
											asyncResultsCount.incrementAndGet();
										}
									}

								}, exec);
							}else{
								session.execute(queryString);
							}
						}catch (Exception e) {
							logger.error("Exception writing to cassandra: " + e.getMessage() + "; keyspace: " + keyspace, e);
							logFailedCql(keyspace, queryString);
							
						}
					}
			}
			
			logger.info("completed: " + requestCount);
			
			if(config.asyncWrites){
				long cqlRequestCount = (requestCount * operationStatements.size());
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
		}finally{
			close(rs);
			close(stmt);
			close(conn);
		}
	}

	void logFailedCql(String keyspaceName, String queryString){
		if(!queryString.trim().endsWith(";")){
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

	public void close(ResultSet rs){
		if(rs != null){
			try {
				rs.close();
			} catch (SQLException e) {
				logger.error("Exception closing resultset: " + e.getMessage(), e);
			}
		}
	}
	
	public void close(Statement stmt){
		if(stmt != null){
			try {
				stmt.close();
			} catch (SQLException e) {
				logger.error("Exception closing statement: " + e.getMessage(), e);
			}
		}
	}
	
	public void close(Connection connection){
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error("Exception closing connection: " + e.getMessage(), e);
			}
		}
	}
}
