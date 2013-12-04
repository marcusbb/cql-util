package driver.em;

import java.io.Serializable;


/**
 * 
 * Most configuration should do with reasonable defaults OR not set at all.
 * 
 */
public class CassConfig implements Serializable {

	private static final long serialVersionUID = 4764982374610680602L;
	
	//Some reasonable testing defaults are supplied, tune for production
	String [] contactHostsName = {"localhost","127.0.0.1"};
	int nativePort = 9042;
	String localDataCenterName = "datacenter1";
	
	//concurrency (per connection)
	int concurrentLocal = 100 ,concurrentRemote = 100;
		
	//connection pooling options
	int coreConnectionsPerLocalHost = 2, maxConnectionsPerLocalHost = 2,
			coreConnectionsPerRemoteHost = 2, maxConnectionsPerRemoteHost = 2;
	
	//reconnect policy
	long baseReconnectDelay = 60000L;
	long maxReconnectDelay = 10 * baseReconnectDelay;

	int connectionTimeoutMs = 5000;
	boolean keepAlive = true;
	int soLinger = 5000;
	boolean tcpNoDelay = false;
	int readTimeoutMs = 120000;
    
    public String[] getContactHostsName() {
		return contactHostsName;
	}
	public void setContactHostsName(String[] contactHostsName) {
		this.contactHostsName = contactHostsName;
	}
	public int getNativePort() {
		return nativePort;
	}
	public void setNativePort(int nativePort) {
		this.nativePort = nativePort;
	}
	public String getLocalDataCenterName() {
		return localDataCenterName;
	}
	public void setLocalDataCenterName(String localDataCenterName) {
		this.localDataCenterName = localDataCenterName;
	}
	public int getCoreConnectionsPerLocalHost() {
		return coreConnectionsPerLocalHost;
	}
	public void setCoreConnectionsPerLocalHost(int coreConnectionsPerLocalHost) {
		this.coreConnectionsPerLocalHost = coreConnectionsPerLocalHost;
	}
	public int getMaxConnectionsPerLocalHost() {
		return maxConnectionsPerLocalHost;
	}
	public void setMaxConnectionsPerLocalHost(int maxConnectionsPerLocalHost) {
		this.maxConnectionsPerLocalHost = maxConnectionsPerLocalHost;
	}
	public int getCoreConnectionsPerRemoteHost() {
		return coreConnectionsPerRemoteHost;
	}
	public void setCoreConnectionsPerRemoteHost(int coreConnectionsPerRemoteHost) {
		this.coreConnectionsPerRemoteHost = coreConnectionsPerRemoteHost;
	}
	public int getMaxConnectionsPerRemoteHost() {
		return maxConnectionsPerRemoteHost;
	}
	public void setMaxConnectionsPerRemoteHost(int maxConnectionsPerRemoteHost) {
		this.maxConnectionsPerRemoteHost = maxConnectionsPerRemoteHost;
	}
	public int getConcurrentLocal() {
		return concurrentLocal;
	}
	public void setConcurrentLocal(int concurrentLocal) {
		this.concurrentLocal = concurrentLocal;
	}
	public void setConcurrentRemote(int concurrentRemote) {
		this.concurrentRemote = concurrentRemote;
	}
	public int getConcurrentRemote() {
    	return concurrentRemote;
    }
	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}
	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}
	public boolean isKeepAlive() {
		return keepAlive;
	}
	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}
	public int getSoLinger() {
		return soLinger;
	}
	public void setSoLinger(int soLinger) {
		this.soLinger = soLinger;
	}
	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}
	public void setTcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}
	public int getReadTimeoutMs() {
		return readTimeoutMs;
	}
	public void setReadTimeoutMs(int readTimeoutMs) {
		this.readTimeoutMs = readTimeoutMs;
	}
	public long getBaseReconnectDelay() {
		return baseReconnectDelay;
	}
	public void setBaseReconnectDelay(long baseReconnectDelay) {
		this.baseReconnectDelay = baseReconnectDelay;
	}
	public long getMaxReconnectDelay() {
		return maxReconnectDelay;
	}
	public void setMaxReconnectDelay(long maxReconnectDelay) {
		this.maxReconnectDelay = maxReconnectDelay;
	}
	
	
}
