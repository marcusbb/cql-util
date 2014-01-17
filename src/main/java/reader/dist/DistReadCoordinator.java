package reader.dist;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

//main coordination class
//determine if it's master
//split the tokens by number of cluster members

public class DistReadCoordinator {
	
	private HazelcastInstance hz;
	
	public DistReadCoordinator(HazelcastInstance hzInst) {
		this.hz = hzInst;
	}
	
	public int getNumMembers() {
		if (hz == null)
			return 0;
		return hz.getCluster().getMembers().size();
	}
	public boolean isMaster() {
		if (hz != null) {
			Member local = hz.getCluster().getLocalMember();
			return local.equals(hz.getCluster().getMembers().iterator().next());
		}
		return false;
	}
}
