package migration;

public enum ExecutorState {

	
	INIT(0), //program is initializing configuration etc
	SQL_EXECUTION(1), //sql is being executed
	RUNNING(2), //main migration is executed
	SUSPENDED(3), //a suspension has been called
	COMPLETED(4); //completed
	
	//other possible transition states
	//RUN_TO_SUSPEND
	//SUSPEND_TO_RUN
	
	private int val;
	private ExecutorState(final int val) {
		this.val = val;
	}
	public int getIntState() {
		return val;
	}
}
