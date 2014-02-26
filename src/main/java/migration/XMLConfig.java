package migration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import driver.em.CassConfig;

@XmlRootElement(name="mapping")
public class XMLConfig {

	private String jdbcUrl;
	
	private String jdbcUsername;
	
	private String jdbcPassword;
	
	private String jdbcDriver;
	
	private String sqlQuery;
	
	private String keyspace;
	
	private CassConfig cassConfig;
	
	List<RSToCqlConfig> rsToCqlConfigs = new ArrayList<>();
	
	boolean asyncWrites;
	
	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public CassConfig getCassConfig() {
		return cassConfig;
	}

	public void setCassConfig(CassConfig cassConfig) {
		this.cassConfig = cassConfig;
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

	public String getJdbcUsername() {
		return jdbcUsername;
	}

	public void setJdbcUsername(String jdbcUsername) {
		this.jdbcUsername = jdbcUsername;
	}

	public String getJdbcPassword() {
		return jdbcPassword;
	}

	public void setJdbcPassword(String jdbcPassword) {
		this.jdbcPassword = jdbcPassword;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public boolean isAsyncWrites() {
		return asyncWrites;
	}

	public void setAsyncWrites(boolean asyncWrites) {
		this.asyncWrites = asyncWrites;
	}

	@Override
	public String toString() {
		return "XMLConfig [jdbcUrl=" + jdbcUrl + ", jdbcUsername="
				+ jdbcUsername + ", jdbcDriver=" + jdbcDriver + ", sqlQuery="
				+ sqlQuery + ", keyspace=" + keyspace + ", cassConfig="
				+ cassConfig + ", rsToCqlConfigs=" + rsToCqlConfigs
				+ ", asyncWrites=" + asyncWrites + "]";
	}
}
