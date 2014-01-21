package reader.dist;

import java.io.Serializable;

import com.hazelcast.core.Member;

public class ReaderResult implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7190588877893715607L;
	
	
	public final Long rowsRead;

	
	public final Member member;
	
	public long execTimeMs;
	
	//A place where clients can do aggregation
	public Serializable source;
	
	public ReaderResult(Long rows,Member member) {
		this.rowsRead = rows;
		this.member = member;
	}
}
