package migration.poc;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="mapping")
public class XMLConfig {

	private String jdbcUrl;
	
	private String jdbcDriver;
	
	private String sqlQuery;
	

	List<RSToCqlConfig> rsToCqlConfigs = new ArrayList<>();
	
	
	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	
	//This is the list of columns
	@XmlElement( name="rsToCqlConfig" )
	@XmlElementWrapper( name="forEach" )
	public List<RSToCqlConfig> getRsToCqlConfigs() {
		return rsToCqlConfigs;
	}

	public void setRsToCqlConfigs(List<RSToCqlConfig> rsToCqlConfigs) {
		this.rsToCqlConfigs = rsToCqlConfigs;
	}

	public void addRsToCqlConfig(RSToCqlConfig rsToCqlConfig) {
		this.rsToCqlConfigs.add(rsToCqlConfig);
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}
	
	
}
