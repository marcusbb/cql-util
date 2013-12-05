package reader;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import driver.em.CassConfig;


@XmlRootElement(name="config")
public class ReaderConfig {

	private CassConfig cassConfig;

	private String table;
	
	private PKConfig pkConfig;
	
	private String []otherCols;
	
	private int pageSize = 1000;
	
	private String readerTask = RowCountTask.class.getName();
	
	
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
	
	

}
