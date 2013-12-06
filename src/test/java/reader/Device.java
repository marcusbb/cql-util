package reader;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="devices")
public class Device {
	public Device() { }
	public Device(String id,String name,String value) {
		this.id = new Id();
		this.id.id = id;
		this.id.name = name;
		this.value = value;
	}
	@EmbeddedId
	public Id id;
	
	public static class Id {
		@Column(name="id")
		public String id;
		@Column(name="name")
		public String name;
	}
	@Column(name="value")
	public String value;
	 
}