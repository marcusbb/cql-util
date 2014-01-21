package reader;



import driver.em.Composite;

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
	private ColumnInfo[] tokenPart;
	//rename to clusterKeys
	private ColumnInfo[] nonTokenPart;
	
	public PKConfig() {
		
	}
	public PKConfig(ColumnInfo []tokenPart,ColumnInfo []nontokenPart) {
		this.tokenPart = tokenPart;
		this.nonTokenPart = nontokenPart;
	}
	public ColumnInfo[] getTokenPart() {
		return tokenPart;
	}


	public void setTokenPart(ColumnInfo[] tokenPart) {
		this.tokenPart = tokenPart;
	}


	public ColumnInfo[] getNonTokenPart() {
		return nonTokenPart;
	}


	public void setNonTokenPart(ColumnInfo[] nonTokenPart) {
		this.nonTokenPart = nonTokenPart;
	}
	
	
	public String getCQLTokenPart() {
		return "token (" + ")";
	}
	

}
