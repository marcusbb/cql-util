# Simple JPA + Cassandra.

The package contained within contains basic mapping functionality for JPA type annotations to the java driver.
Supported are a the @Id, @EmbeddedId and @Column annotations, in a simplistic way.

No support for JOINS, or any entity to entity related mapping.

Data types supported:
- String
- long, integer
- timestamp
- Bytebuffer (byte[])
- Map<String,String>

```java
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
	//Currently only Map<String,String> is supported!!
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
entityMgr.persist(obj);

//find by pk
entityMgr.findby(objId);

//remove
entityMgr.remove(objId);

//with prepared statement cache
//A convenience request parameters
CUtils.getDefaultPSCacheParams() //retrieves parameters that will use a prepared statement cache

DefaultEntityManager<IdClass, EntityClass> em = new DefaultEntityManager<>(session, EntityClass.class,CUtils.getDefaultPSCacheParams());

//Batch Manager

BatchManager bm = new DefaultBatchManager(session, List of classes to write);
bm.batchWrite(CUtils.getDefaultParams(), list of entities to write);

		
```

Key Reference classes
[JPA](http://docs.oracle.com/javaee/5/api/javax/persistence/package-summary.html) - Only limited annotation implementation (Entity, Id, Column) 
[EntityManager](EntityManager.java) - Equivalent of JPA entity manager
[CUtils](CUtils.java) - configure your cluster, and get default request parameters
[DefaultEntityManager](DefaultEntityManager.java) - Direct convenience use class for instantiating EntityManager, or extend for convenience

