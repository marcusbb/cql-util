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
	
	
	//this should be abstract - definitely over-ridden
	public abstract RowReaderTask newTask() throws Exception;
	
	
}
