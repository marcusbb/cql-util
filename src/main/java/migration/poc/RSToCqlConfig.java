package migration.poc;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

public class RSToCqlConfig {

	private String cqlTable;
	
	private List<JdbcColMapping> columns;

	//If these are available then 
	//This is for the mapping to a "map type" in cassandra
	private JdbcColMapping nameMapping;
	
	private JdbcColMapping valueMapping;
	
	public RSToCqlConfig() {}
	
	public RSToCqlConfig(List<JdbcColMapping> columns) {
		this.columns = columns;
	}

	//This is the list of columns
	@XmlElement( name="column" )
	@XmlElementWrapper( name="columns" )
	public List<JdbcColMapping> getColumns() {
		return columns;
	}

	public void setColumns(List<JdbcColMapping> columns) {
		this.columns = columns;
	}

	public JdbcColMapping getNameMapping() {
		return nameMapping;
	}

	public void setNameMapping(JdbcColMapping nameMapping) {
		this.nameMapping = nameMapping;
	}

	public JdbcColMapping getValueMapping() {
		return valueMapping;
	}

	public void setValueMapping(JdbcColMapping valueMapping) {
		this.valueMapping = valueMapping;
	}

	public String getCqlTable() {
		return cqlTable;
	}

	public void setCqlTable(String cqlTable) {
		this.cqlTable = cqlTable;
	}

	

}
