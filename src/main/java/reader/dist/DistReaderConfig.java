package reader.dist;

import java.io.Serializable;

import com.hazelcast.core.Member;

import reader.ReaderConfig;

public class DistReaderConfig extends ReaderConfig implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8201668723079862175L;

	//get the name of the hazelcast instance
	private String hzInstName;

	private Member targetMember;
	
	public String getHzInstName() {
		return hzInstName;
	}

	public void setHzInstName(String hzInstName) {
		this.hzInstName = hzInstName;
	}

	public Member getTargetMember() {
		return targetMember;
	}

	public void setTargetMember(Member targetMember) {
		this.targetMember = targetMember;
	}
	
	
	
}
