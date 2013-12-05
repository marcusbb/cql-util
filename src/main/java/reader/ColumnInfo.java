package reader;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import com.datastax.driver.core.DataType;

import driver.em.CUtils;

public class ColumnInfo {
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