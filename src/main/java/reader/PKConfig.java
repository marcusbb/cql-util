package reader;



import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;

import driver.em.CUtils;

/**
 *  
 * This is the primary key configuration
 *
 * I could read this information from the system table, 
 * but we'll make it explicit for now.
 * 
 */
public class PKConfig {

	//currently token can only have one part
	//rename this to paritionKeys - as a limit of 1 at the moment due to CQL issue
	private ColumnInfo[] partitionKeys;
	//rename to clusterKeys
	private ColumnInfo[] clusterKeys;
	//may not not be needed as an XML config object
	public static class ColumnInfo {
		
		public ColumnInfo(ColumnMetadata metaData) {
			this.name =metaData.getName();
			this.type = metaData.getType();
		}
		public ColumnInfo() {}
		public ColumnInfo(String name,DataType type) {
			this.name = name;
			this.type = type;
		}
		String name;
		@XmlTransient
		DataType type;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		@XmlElement(name="type")
		public String getTypeInStr() {
			return type.getName().name().toString();
		}
		
		public void setTypeInStr(String type) {
			this.type = CUtils.Name.parseType(type);
			
		}
	}
	public PKConfig() {
		
	}
	public PKConfig(ColumnInfo []tokenPart,ColumnInfo []nontokenPart) {
		this.partitionKeys = tokenPart;
		this.clusterKeys = nontokenPart;
	}
	public ColumnInfo[] getPartitionKeys() {
		return partitionKeys;
	}


	public void setPartitionKeys(ColumnInfo[] tokenPart) {
		this.partitionKeys = tokenPart;
	}


	public ColumnInfo[] getClusterKeys() {
		return clusterKeys;
	}


	public void setClusterKeys(ColumnInfo[] nonTokenPart) {
		this.clusterKeys = nonTokenPart;
	}
	
	
	public String getCQLTokenPart() {
		return "token (" + ")";
	}
	

}
