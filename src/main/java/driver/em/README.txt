The package contained within contains basic mapping functionality for JPA type annotations
to the java driver.
Supported are a the @Id, @EmbeddedId and @Column annotations, in a simplistic way.

Data types supported:
- String
- long, integer
- timestamp
- Bytebuffer (byte[])
- Map<String,String>


Sample Entity -with an embedded composite key
@Entity
@Table(name="user_files_v")
public class UserFile {

	public UserFile() {}
	
	public UserFile(Id id) {
		this.id = id;
	}
	@EmbeddedId
	public Id id;
	
	public static class Id {
		public Id() {}
		public Id(String username,String filename,long version) {
			this.username = username;
			this.filename = filename;
			this.version = version;
		}
		@Column(name="username")
		public String username;
		
		@Column(name="filename")
		public String filename;
		
		@Column(name="version")
		public long version;
	}
	
	@Column(name="properties")
	public Map<String,String> properties;
	
	@Column(name="state")
	public Integer state;
	
	@Column(name="create_date")
	public Date createDate;
	
	@Column(name="file_blob")
	public ByteBuffer fileBlob;
	
	@Column(name="fileid")
	public Long fileAltId;
	

}

//Usage:
DefaultEntityManager<UserFile.Id,UserFile> entityMgr = new DefaultEntityManager<>();
//write
entityMgr.persist(obj,CUtils.getDefaultParams());

//find by pk
entityMgr.findby(objId,CUtils.getDefaultParams());

TODO:
Add ability to batch statements together
