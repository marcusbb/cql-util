package reader;

import java.io.Serializable;

/**
 * 
 * For distributed Reader jobs its important that subclasses are {@link Serializable}
 *
 */
public abstract class ReaderJob implements Serializable {

	
	private static final long serialVersionUID = 3937480396463424314L;

	protected long startTimeMs;
	
	protected long endTimeMs;
	
	
	public ReaderJob() {
		this.startTimeMs = System.currentTimeMillis();
	}
	
	
	/**
	 * Define a new task or pooled task - should be thread safe
	 * 
	 * @return
	 * @throws Exception
	 */
	public abstract RowReaderTask newTask() throws Exception;
	
	
}
