package driver.em;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="id_only")
public class IdOnlyEntity {

	@EmbeddedId
	public Id id;
	
	public IdOnlyEntity() {}
	public IdOnlyEntity(Id id) {
		this.id = id;
	}
		
	public static class Id {
		public Id() {}
		public Id(String first,Long second) {
			this.firstPart = first;
			this.secondPart = second;
		}
		@Column(name="first")
		public String firstPart;
		
		@Column(name="second")
		public Long secondPart;
		
	}
}
