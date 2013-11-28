package driver.em;

import java.util.Map;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="sample")
public class SampleEmbeddedEntity {

	public static class Id {
		public Id(String a,String b) {
			this.partA = a;
			this.partB = b;
		}
		@Column(name="a")
		public String partA;
		@Column(name="b")
		public String partB;
		
	}
	//must have a default constructor
	public SampleEmbeddedEntity() {
		
	}
	public SampleEmbeddedEntity(Id id) {
		this.id = id;
	}
	@EmbeddedId
	public Id id;
	
	@Column(name="col1")
	public String colStr;
	
	@Column(name="properties")
	public Map<String, String> properties;

}
