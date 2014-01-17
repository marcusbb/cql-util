package reader.dist;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import reader.ReaderConfig;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;

//main coordination class
//determine if it's master
//split the tokens by number of cluster members
//create new ReaderConfigs for each distributed run

public class DistReadCoordinator {
	
	private HazelcastInstance hz;
	private static long _end = Long.MAX_VALUE;
	private static long _start = Long.MIN_VALUE;
	
	
	public DistReadCoordinator(HazelcastInstance hzInst) {
		this.hz = hzInst;
	}
	/**
	 * Executes a complete row execution distributed equally amongst the 
	 * HZ members.
	 * 
	 * @param execService
	 */
	public List<CompletionCallback> execute(String execName) {
		
		IExecutorService execService = hz.getExecutorService(execName);
		
		//for each member we execute on it passing in each configuration
		//on start and end keys
		DistReaderConfig []configs = getReaderConfig();
		List<CompletionCallback> callbackResults= new ArrayList<>();
		for (int i=0;i<configs.length;i++) {
			CompletionCallback callback = new CompletionCallback();
			callbackResults.add(callback);
			execService.submitToMember(new DistReadTask(configs[i]), configs[i].getTargetMember(),callback);
		}
		return callbackResults;
	}
	
	public DistReaderConfig [] getReaderConfig() {
		int num = getNumMembers();
		DistReaderConfig[] configs = new DistReaderConfig[num];
		
		Long delta = (_end)/num *2;
		long next = _start;	
		Iterator<Member> memberIter = hz.getCluster().getMembers().iterator();
		
		for (int i=0;i<num;i++) {
			Member member = memberIter.next();
			configs[i] = new DistReaderConfig();
			configs[i].setTargetMember(member);
			configs[i].setStartToken(next);
			if (i<num-1)
				configs[i].setEndToken(next + delta -1);
			else
				configs[i].setEndToken(_end);
			
			next = next + delta;
		}
		
		return configs;
		
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
