package migration;

import static driver.em.CharConst.and;
import static driver.em.CharConst.comma;
import static driver.em.CharConst.eqParam;
import static driver.em.CharConst.space;
import static driver.em.CharConst.ques;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.SimpleStatement;

public class RowToCql {

	protected ResultSet rs;
	
	public JdbcColMapping[] colMapping;
	
	public String cqlTable;
	
	public String keyspace;
	
	public RowToCql(ResultSet rs,String cqlTable,JdbcColMapping []mapping) {
		this.rs = rs;
		this.cqlTable = cqlTable;
		this.colMapping = mapping;
	}
	
	public RowToCql(ResultSet rs, String cqlTable,JdbcColMapping []mapping, String keyspace) {
		this.rs = rs;
		this.cqlTable = cqlTable;
		this.colMapping = mapping;
		this.keyspace = keyspace;
	}
	
	public String getCQL() throws SQLException {
		
		StringBuilder columnsBuilder = new StringBuilder();
		StringBuilder valuesBuilder = new StringBuilder();
		
		//TODO: exclude columns not in the resultset: rs
		for (JdbcColMapping mapping: colMapping) {
			columnsBuilder.append(mapping.cqlName).append(comma).append(space);
			valuesBuilder.append(ques).append(comma).append(space);
		}
		
		columnsBuilder.delete(columnsBuilder.lastIndexOf(","), columnsBuilder.length());
		valuesBuilder.delete(valuesBuilder.lastIndexOf(","), valuesBuilder.length());
		
		StringBuilder builder = new StringBuilder("INSERT INTO ").append(cqlTable)
		.append(" ( ").append(columnsBuilder).append(" ) ")
		.append(" VALUES ( ").append(valuesBuilder).append(" ) ");
		
		return builder.toString();
	}
	
	public Object[] values() throws SQLException {
		List<Object> values = new ArrayList<>();
		
		for (JdbcColMapping mapping: colMapping) {
				values.add(getValue(mapping));	
		}
		
		return values.toArray();
	}
	
	//this is where the conversion magic happens
	public Object getValue(JdbcColMapping mapping) throws SQLException {
		
		Object value = null;
		if (DataType.text().equals(mapping.type)) {
			value = rs.getString(mapping.jdbcName);
			
		}else if (DataType.ascii().equals(mapping.type)) {
			value = rs.getString(mapping.jdbcName);
		} 
		else if (DataType.cint().equals(mapping.type)) {
			value = rs.getInt(mapping.jdbcName);
		} else if (DataType.bigint().equals(mapping.type)) {
			value = rs.getLong(mapping.jdbcName);
		}		
		else if (DataType.timestamp().equals(mapping.type)) {
			value = rs.getTimestamp(mapping.jdbcName);
		}else if (DataType.blob().equals(mapping.type)) {
			value = rs.getBytes(mapping.jdbcName);
		}
		//what if value is null? primitives won't like this
		return value;
		
	}
	public String getIdPredicate() {
		StringBuilder builder = new StringBuilder();
		builder.append(" where ");
		
		for (JdbcColMapping mapping: colMapping) {
			if (mapping.isPK) {
				builder.append(mapping.cqlName).append(eqParam);
				builder.append(space).append(and).append(space);
			}
		}
		//remove trailing "and"
		builder.replace(builder.length()-4, builder.length()-1, "");
		return builder.toString();
	}
	
	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public SimpleStatement getStatement() throws SQLException {
		
		SimpleStatement ss = new SimpleStatement(getCQL(),values());
		
		return ss;
	}
	
}
