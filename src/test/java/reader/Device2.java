package reader;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="devices2")
public class Device2 {
	public Device2() { }
	public Device2(String id,String name,String type, String value) {
		this.id = new Id();
		this.id.id = id;
		this.id.name = name;
		this.id.type = type;
		this.value = value;
	}
	@EmbeddedId
	public Id id;
	
	public static class Id {
		@Column(name="id")
		public String id;
		@Column(name="type")
		public String type;
		
		@Column(name="name")
		public String name;
	}
	@Column(name="value")
	public String value;
	 
}