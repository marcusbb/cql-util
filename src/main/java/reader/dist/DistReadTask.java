package reader.dist;

import java.util.concurrent.Callable;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import reader.CQLRowReader;
import reader.ReaderJob;


public class DistReadTask implements Callable<ReaderResult>{

	private DistReaderConfig readerConfig;
	
	private ReaderJob job;
	
	public DistReadTask(DistReaderConfig readerConfig,ReaderJob job) {
		this.readerConfig = readerConfig;
		this.job = job;
		
		
	}
	@Override
	public ReaderResult call() throws Exception {
		
				
		//the main show
		CQLRowReader reader = new CQLRowReader(readerConfig, job);
		long start = System.currentTimeMillis();
		reader.read();
		long delta = System.currentTimeMillis() -start;
		
		this.readerConfig.getHzInstName();
		Member member = Hazelcast.getHazelcastInstanceByName(readerConfig.getHzInstName()).getCluster().getLocalMember();
		
		ReaderResult result = new ReaderResult( reader.getTotalReadCount(), member);
		result.execTimeMs = delta;
		
		return result;
	}

	
}
