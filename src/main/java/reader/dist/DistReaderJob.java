package reader.dist;

import java.util.List;

import reader.ReaderJob;

/**
 * 
 * 
 *
 */
public abstract class DistReaderJob<V> extends ReaderJob<V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7806390257491765360L;

	
	public abstract CompletionCallback newCompletionCallBack();
	
	
	//I'm not sure this is necessary
	//public abstract void performCompletion(List<CompletionCallback> listCallbacks);
}
