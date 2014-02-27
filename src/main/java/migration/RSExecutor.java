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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import driver.em.SimpleStatement;

public class RSExecutor {
   
	protected Map<String, Session> sessions;
	static Logger logger =  LoggerFactory.getLogger(RSExecutor.class);
	XMLConfig config;
	
	private static long defaultKeepAlive = 60; 
	private static TimeUnit defaultKeepAliveTU = TimeUnit.SECONDS;
	private static int defaultQueueCapacity = 1000;
	private static int corePoolSize = 5;
	private static int maxPoolSize = 10;
	
	public RSExecutor(XMLConfig config, Session session) {
		this.config = config;
		this.sessions = new HashMap<String, Session>();
		this.sessions.put(config.getKeyspace(), session);
	}
	
	public RSExecutor(XMLConfig config, Map<String, Session> sessions) {
		this.config = config;
		this.sessions = sessions;
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
			
			//TODO replace this with a JMX metric.
			int count = 0;
			
			ThreadPoolExecutor exec = new  ThreadPoolExecutor(corePoolSize, maxPoolSize, defaultKeepAlive,  defaultKeepAliveTU,  new ArrayBlockingQueue<Runnable>(defaultQueueCapacity));
			while (rs.next()) {
				try {
					if (count++ % 1000 == 0){
						logger.info("completed: " + count);
					}

					for (RowToCql row: operationStatements) {
						final String keyspace = row.getKeyspace() != null?row.getKeyspace():config.getKeyspace();
						Session session = sessions.get(keyspace);
						if(config.asyncWrites){
							final String queryString = row.getStatement().buildQueryString();
							final ResultSetFuture future = session.executeAsync(queryString);
							future.addListener( new Runnable(){
								@Override
								public void run() {
									try {
										future.get();
									} catch (InterruptedException e) {
										logger.error("InterruptedException: " + e.getMessage() + "; keyspace: " + keyspace + "; statement: " + queryString);
									} catch (ExecutionException e) {
										logger.error("ExecutionException: " + e.getMessage() + "; keyspace: " + keyspace + "; statement: " + queryString);
									}
								}
								
							}, exec);
						}else{
							session.execute(
									row.getStatement().buildQueryString()
									);
						}
					}

				}catch (Exception e) {
					//TODO log it properly so it can be retried for later
					logger.error("Exception writing to cassandra: " + e.getMessage(), e);
				}
			}
			
		}finally{
			close(rs);
			close(stmt);
			close(conn);
		}
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
