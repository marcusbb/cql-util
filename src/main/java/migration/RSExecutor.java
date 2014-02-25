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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

public class RSExecutor {
    
	protected Map<String, Session> sessions;
	static Logger logger =  LoggerFactory.getLogger(RSExecutor.class);
	public static String DEFAULT_KEY = "default";
	XMLConfig config;
	
	public RSExecutor(XMLConfig config, Session session) {
		this.config = config;
		this.sessions = new HashMap<String, Session>();
		this.sessions.put(DEFAULT_KEY, session);
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
	
	public void execute() throws SQLException, ClassNotFoundException {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs =  null;
		
		try{
			conn = getJdbcConnection();
			
			stmt = conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			long startExec = System.currentTimeMillis();
			rs = stmt.executeQuery(config.getSqlQuery());
			long endExec = System.currentTimeMillis();
			long elapsed = endExec-startExec;

			System.out.println("Completed SQL execution: " + config.getSqlQuery() + " in (MS)" +elapsed );
			logger.info("Completed SQL execution: " + config.getSqlQuery() + " in (MS)" +elapsed );

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
			//TODO replace this with a JMX metric.
			int count = 0;
			while (rs.next()) {

				try {
					if (count++ % 1000 == 0)
						System.out.println("completed: " + count);

					for (RowToCql row: operationStatements) {
						Session session = (row.getKeyspace() != null)?sessions.get(row.getKeyspace()):sessions.get(DEFAULT_KEY);
						session.execute(
								row.getStatement().buildQueryString()
								);
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
