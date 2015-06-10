package migration;

public interface RSExecutorMBean {

	void suspendForMins(int mins);
	
	void suspend();
	
	void resume();
	
	String getStateAsString();
	
}
