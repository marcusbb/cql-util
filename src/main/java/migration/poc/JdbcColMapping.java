package migration.poc;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import com.datastax.driver.core.DataType;

import driver.em.CUtils;

@XmlType(name="column")
public class JdbcColMapping {

	//for xml marshalling purposes
	public JdbcColMapping() {}
	/**
	 * jdbc and cql names are the same
	 * 
	 * @param jdbcName
	 * @param type
	 */
	public JdbcColMapping(String jdbcName,DataType type) {
		this(jdbcName,jdbcName,type,false);
	}
	public JdbcColMapping(String jdbcName,String cqlName,DataType type,boolean isPK) {
		this.jdbcName = jdbcName;
		this.cqlName = cqlName;
		this.type = type;
		this.isPK = isPK;
	}
	public String jdbcName;
	
	public String cqlName;
	@XmlTransient
	public DataType type;
	
	public boolean isPK = false;
	
	@XmlElement(name="type")
	public String getTypeInStr() {
		return type.getName().name().toString();
	}
	
	public void setTypeInStr(String type) {
		this.type = CUtils.Name.parseType(type);
		
	}
	
	
}
