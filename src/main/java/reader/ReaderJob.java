package reader;

import java.io.Serializable;

/**
 * 
 * For distributed Reader jobs its important that subclasses are {@link Serializable}
 *
 */
public abstract class ReaderJob<V> implements Serializable,IReaderJob<V> {

	
	private static final long serialVersionUID = 3937480396463424314L;

	protected long startTimeMs;
	
	protected long endTimeMs;
	
	
	public ReaderJob() {
		this.startTimeMs = System.currentTimeMillis();
	}
	
	
	/**
	 * Define a new task or pooled task 
	 * 
	 * @return
	 * @throws Exception
	 */
	public abstract RowReaderTask<V> newTask() throws Exception;
	
	public abstract void processResult(V result);
	
	/**
	 * The read job is complete
	 */
	public abstract void onReadComplete();
}
