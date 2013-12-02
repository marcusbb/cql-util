package migration.poc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class RSExecutor {

	protected Session session;
	
	
	XMLConfig config;
	
	public RSExecutor(XMLConfig config,Session session) {
		this.config = config;
		this.session = session;
	}
	
	public void execute() throws SQLException {
		Connection conn = null;
		
		
		conn = DriverManager.getConnection(config.getJdbcUrl());
				
		Statement stmt = conn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		long startExec = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(config.getSqlQuery());
		long endExec = System.currentTimeMillis();
		
		System.out.println("Completed SQL execution: " + config.getSqlQuery() + " in (MS)" +(endExec-startExec) );
		
		List<RowToCql> operationStatements = new ArrayList<>();
		//build the Row and RowtoMap
		for (RSToCqlConfig cqlConfig:config.rsToCqlConfigs) {
			RowToCql rowToCql = null;
			if (cqlConfig.getNameMapping() != null)
				rowToCql = new RowToCqlMap(rs, cqlConfig.getCqlTable(), cqlConfig.getColumns().toArray(new JdbcColMapping[]{}),cqlConfig.getNameMapping(),cqlConfig.getValueMapping());
			else
				rowToCql = new RowToCql(rs, cqlConfig.getCqlTable(), cqlConfig.getColumns().toArray(new JdbcColMapping[]{}) );
			operationStatements.add(rowToCql);
		}
		//TODO replace this with a JMX metric.
		
		int count = 0;
		while (rs.next()) {
			
			try {
				if (count++ % 1000 == 0)
					System.out.println("completed: " + count);
				
				for (RowToCql row: operationStatements) {
					session.execute(
							row.getStatement()
							);
				}
				
			}catch (Exception e) {
				//log it properly so it can be retried for later
				e.printStackTrace();
			}
		}
		
	}
}
