package reader.dist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reader.CQLRowReader;

import com.hazelcast.core.ExecutionCallback;

public class CompletionCallback implements ExecutionCallback<ReaderResult>{

	private static Logger logger = LoggerFactory.getLogger(CQLRowReader.class);
	
	
	@Override
	public void onFailure(Throwable t) {
		logger.error(t.getMessage(), t);
		
	}

	@Override
	public void onResponse(ReaderResult arg0) {
		// TODO Auto-generated method stub
		
	}

	
}
