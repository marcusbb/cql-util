package migration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.datastax.driver.core.ConsistencyLevel;

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
	
	int batchWrites;
	
	ConsistencyLevel consistencyLevel;
	
	//used for testing purposes only
	boolean bypassCassandra;
	
	private long tpKeepAlive = 60; 
	private TimeUnit tpKeepAliveTU = TimeUnit.SECONDS;
	private int tpQueueCapacity = 1000;
	private int corePoolSize = 5;
	private int maxPoolSize = 10;
	
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

	public int getBatchWrites() {
		return batchWrites;
	}

	public void setBatchWrites(int batchWrites) {
		this.batchWrites = batchWrites;
	}

	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	public boolean isBypassCassandra() {
		return bypassCassandra;
	}

	public void setBypassCassandra(boolean bypassCassandra) {
		this.bypassCassandra = bypassCassandra;
	}

	public long getTpKeepAlive() {
		return tpKeepAlive;
	}

	public void setTpKeepAlive(long tpKeepAlive) {
		this.tpKeepAlive = tpKeepAlive;
	}

	public TimeUnit getTpKeepAliveTU() {
		return tpKeepAliveTU;
	}

	public void setTpKeepAliveTU(TimeUnit tpKeepAliveTU) {
		this.tpKeepAliveTU = tpKeepAliveTU;
	}

	public int getTpQueueCapacity() {
		return tpQueueCapacity;
	}

	public void setTpQueueCapacity(int tpQueueCapacity) {
		this.tpQueueCapacity = tpQueueCapacity;
	}

	public int getCorePoolSize() {
		return corePoolSize;
	}

	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	@Override
	public String toString() {
		return "XMLConfig [jdbcUrl=" + jdbcUrl + ", jdbcUsername="
				+ jdbcUsername + ", jdbcPassword=" + jdbcPassword
				+ ", jdbcDriver=" + jdbcDriver + ", sqlQuery=" + sqlQuery
				+ ", keyspace=" + keyspace + ", cassConfig=" + cassConfig
				+ ", rsToCqlConfigs=" + rsToCqlConfigs + ", asyncWrites="
				+ asyncWrites + ", batchWrites=" + batchWrites
				+ ", consistencyLevel=" + consistencyLevel
				+ ", bypassCassandra=" + bypassCassandra + ", tpKeepAlive="
				+ tpKeepAlive + ", tpKeepAliveTU=" + tpKeepAliveTU
				+ ", tpQueueCapacity=" + tpQueueCapacity + ", corePoolSize="
				+ corePoolSize + ", maxPoolSize=" + maxPoolSize + "]";
	}

	

	

}
