package reader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.datastax.driver.core.ConsistencyLevel;

import reader.PKConfig.ColumnInfo;
import driver.em.CassConfig;


@XmlRootElement(name="config")
public class ReaderConfig implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6130085012694365732L;

	private CassConfig cassConfig = new CassConfig();

	private String keyspace;
	
	private String table;
	
	private PKConfig pkConfig = new PKConfig();
	
	private String []otherCols;
	
	
	//this should be a large number - probably about 1000 + (and depending on your row CQL PK row sizes )
	private int pageSize = 1000;
	
	//For a single job configuration
	private Long startToken = Long.MIN_VALUE;
	
	private Long endToken = Long.MAX_VALUE;
	
	//this is for multi-threaded read
	private int numThreads = 1;
	
	private TokenRange []tokenRanges;
	
	private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	
	public static class TokenRange {
		
		public TokenRange(Long start,Long end) {
			this.startToken = start;
			this.endToken = end;
		}
		private Long startToken = Long.MIN_VALUE;
		
		private Long endToken = Long.MAX_VALUE;

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
			return "TokenRange [startToken=" + startToken + ", endToken="
					+ endToken + "]";
		}
		
		
		
	}
	//end multi-threaded config
	
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

	public List<ColumnInfo> getPKColumns() {
		ArrayList<ColumnInfo> allCols = new ArrayList<>();
		if (pkConfig != null && pkConfig.getPartitionKeys() !=null)
			for (ColumnInfo inf:pkConfig.getPartitionKeys()) {
				allCols.add(inf);
			}
		if (pkConfig != null && pkConfig.getClusterKeys() !=null)
			for (ColumnInfo inf:pkConfig.getClusterKeys()) {
				allCols.add(inf);
			}
		
		return allCols;
	}
	
	public int getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}

	public TokenRange[] getTokenRanges() {
		return tokenRanges;
	}

	public void setTokenRanges(TokenRange[] tokenRanges) {
		this.tokenRanges = tokenRanges;
	}
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	@Override
	public String toString() {
		return "ReaderConfig [cassConfig=" + cassConfig + ", keyspace="
				+ keyspace + ", table=" + table + ", pkConfig=" + pkConfig
				+ ", otherCols=" + Arrays.toString(otherCols) + ", pageSize="
				+ pageSize + ", startToken=" + startToken + ", endToken="
				+ endToken +  "]";
	}
	
	

}
