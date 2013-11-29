package driver.em;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="sample")
public class SampleEntity {

	protected String mocked = null;
	//must have a default constructor
	public SampleEntity() {
		
	}
	public SampleEntity(String id) {
		this.id = id;
	}
	@Id
	@Column(name="key")
	public String id;
	
	@Column(name="col1")
	public String simpleCol;
	
	@Column(name="properties")
	public Map<String, String> properties;
	
	@Column(name="col2")
	public Long ts;
	
	@Column(name="date_col")
	public Date date;
	
	@Column(name="b_info")
	public ByteBuffer blob;
	
	public String getSimpleCol() {
		return simpleCol;
	}
	public void setSimpleCol(String simpleCol) {
		this.simpleCol = simpleCol;
		mocked = simpleCol;
	}

	
	
}
