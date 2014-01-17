package reader;

import java.io.Serializable;
import java.util.Arrays;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import driver.em.CassConfig;


@XmlRootElement(name="config")
public class ReaderConfig implements Serializable {

	private CassConfig cassConfig;

	private String keyspace;
	
	private String table;
	
	private PKConfig pkConfig;
	
	private String []otherCols;
	
	//and MUST be larger than any column family row
	//this should be a large number - probably about 1000 + (and depending on your row CQL PK row sizes )
	private int pageSize = 1000;
	
	private Long startToken = Long.MIN_VALUE;
	
	private Long endToken = Long.MAX_VALUE;
	
	private String readerTask = LoggingRowTask.class.getName();
	
	
	public CassConfig getCassConfig() {
		return cassConfig;
	}

	public void setCassConfig(CassConfig cassConfig) {
		this.cassConfig = cassConfig;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
	
	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public PKConfig getPkConfig() {
		return pkConfig;
	}

	public void setPkConfig(PKConfig pkConfig) {
		this.pkConfig = pkConfig;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public String[] getOtherCols() {
		return otherCols;
	}

	public void setOtherCols(String[] otherCols) {
		this.otherCols = otherCols;
	}

	public String getReaderTask() {
		return readerTask;
	}

	public void setReaderTask(String readerTask) {
		this.readerTask = readerTask;
	}

	public Long getStartToken() {
		return startToken;
	}

	public void setStartToken(Long startToken) {
		this.startToken = startToken;
	}

	public Long getEndToken() {
		return endToken;
	}

	public void setEndToken(Long endToken) {
		this.endToken = endToken;
	}

	@Override
	public String toString() {
		return "ReaderConfig [cassConfig=" + cassConfig + ", keyspace="
				+ keyspace + ", table=" + table + ", pkConfig=" + pkConfig
				+ ", otherCols=" + Arrays.toString(otherCols) + ", pageSize="
				+ pageSize + ", startToken=" + startToken + ", endToken="
				+ endToken + ", readerTask=" + readerTask + "]";
	}
	
	

}
