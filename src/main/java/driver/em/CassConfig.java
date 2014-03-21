package driver.em;

import java.io.Serializable;
import java.util.Arrays;


/**
 * 
 * Most configuration should do with reasonable defaults OR not set at all.
 * Rename this to DefaultCassConfig
 */
public class CassConfig implements Serializable {

	private static final long serialVersionUID = 4764982374610680602L;
	
	public enum LoadBalancing {
		TOKEN_AWARE_DC_RR,
		TOKEN_AWARE_RR,
		ROUND_ROBIN;
		
	}
	//Some reasonable testing defaults are supplied, tune for production
	String [] contactHostsName = {"localhost","127.0.0.1"};
	int nativePort = 9042;
	String localDataCenterName = "datacenter1";
	String username;
	String password;
	
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
    
	LoadBalancing loadBalancing = LoadBalancing.TOKEN_AWARE_DC_RR;
	
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

	@Override
	public String toString() {
		return "CassConfig [contactHostsName="
				+ Arrays.toString(contactHostsName) + ", nativePort="
				+ nativePort + ", localDataCenterName=" + localDataCenterName
				+ ", concurrentLocal=" + concurrentLocal
				+ ", concurrentRemote=" + concurrentRemote
				+ ", coreConnectionsPerLocalHost="
				+ coreConnectionsPerLocalHost + ", maxConnectionsPerLocalHost="
				+ maxConnectionsPerLocalHost
				+ ", coreConnectionsPerRemoteHost="
				+ coreConnectionsPerRemoteHost
				+ ", maxConnectionsPerRemoteHost="
				+ maxConnectionsPerRemoteHost + ", baseReconnectDelay="
				+ baseReconnectDelay + ", maxReconnectDelay="
				+ maxReconnectDelay + ", connectionTimeoutMs="
				+ connectionTimeoutMs + ", keepAlive=" + keepAlive
				+ ", soLinger=" + soLinger + ", tcpNoDelay=" + tcpNoDelay
				+ ", readTimeoutMs=" + readTimeoutMs + "]";
	}
	

	public LoadBalancing getLoadBalancing() {
		return loadBalancing;
	}
	public void setLoadBalancing(LoadBalancing loadBalancing) {
		this.loadBalancing = loadBalancing;
	}

	
	public String getUsername() {
		return username;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	@Override
	public String toString() {
		return "CassConfig [contactHostsName="
				+ Arrays.toString(contactHostsName) + ", nativePort="
				+ nativePort + ", localDataCenterName=" + localDataCenterName
				+ ", username=" + username
				+ ", concurrentLocal=" + concurrentLocal
				+ ", concurrentRemote=" + concurrentRemote
				+ ", coreConnectionsPerLocalHost="
				+ coreConnectionsPerLocalHost + ", maxConnectionsPerLocalHost="
				+ maxConnectionsPerLocalHost
				+ ", coreConnectionsPerRemoteHost="
				+ coreConnectionsPerRemoteHost
				+ ", maxConnectionsPerRemoteHost="
				+ maxConnectionsPerRemoteHost + ", baseReconnectDelay="
				+ baseReconnectDelay + ", maxReconnectDelay="
				+ maxReconnectDelay + ", connectionTimeoutMs="
				+ connectionTimeoutMs + ", keepAlive=" + keepAlive
				+ ", soLinger=" + soLinger + ", tcpNoDelay=" + tcpNoDelay
				+ ", readTimeoutMs=" + readTimeoutMs + ", loadBalancing="
				+ loadBalancing + "]";
	}
}
