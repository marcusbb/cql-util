package reader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;

import reader.PKConfig.ColumnInfo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryTimeoutException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import driver.em.CUtils;
import driver.em.Composite;

public class CQLRowReader<V> {

	protected Cluster cluster;
	
	protected Session session;
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(CQLRowReader.class);
	
	ReaderConfig config;
	
	ReaderJob<V> job;
	
	long totalReadCount = 0;
	
	
	public CQLRowReader(ReaderJob<V> job) {
		this.job = job;
	}
	
	
	public CQLRowReader(ReaderConfig config,ReaderJob<V> job,Cluster cluster,Session session) {
		this.cluster = cluster;
		this.session = session;
		this.job = job;
		this.config = config;
	}
	/**
	 * Read from the configured start and end
	 */
	public void read() {
		Long startToken = config.getStartToken();
		
		Long endToken = config.getEndToken();
		
		read(startToken,endToken);
	}
	
	@SuppressWarnings("unchecked")
	public void read(final Long startOfToken,final Long endToken) {
		boolean more = true;
		
		//start at the beginning of the token range.
						
		int pageSize = config.getPageSize();
		
		long startToken = startOfToken;
		
		//HashSet container to keep track of all of items in the last fetch
		//it's compared to a current page count to exclude duplicates from previous
		//query
		HashSet<Object> lastIdSet = new HashSet<Object>(pageSize);
		ByteBuffer[] routeKey = null;
		
		while (more) {
			
			String cql = generateSelectPrefix(startToken,endToken) ;
			SimpleStatement ss = new SimpleStatement( cql );
			ss.setConsistencyLevel(config.getConsistencyLevel());
			if (routeKey != null) 
				ss.setRoutingKey(routeKey);
			//ss.setRoutingKey(routeKey);
			logger.debug("Executing cql: {} , routeKey: {} " ,cql, startToken);
			try {
				ResultSet rs = session.execute(ss);
				
				Iterator<Row> iter = rs.iterator();
				
				//bail on empty result set
				if (!iter.hasNext())
					break;
				//hold the last row id (composite key)
				Object lastId = null;
				
				Row row = null;
				int curRowCount = 0;
				HashSet<Object> curIdSet = new HashSet<Object>(pageSize);
				
							
				//THROUGH THE ROW RESULT - per start token
				long curReadCount = 0;
				while (iter.hasNext()) {
					curRowCount++;
					row = iter.next();
					
					lastId = getRowCompositeKey(row);
					//logger.debug("lastId {}, contained {}" + lastId , lastIdSet.contains(lastId));
					
					curIdSet.add(lastId);
					if (lastIdSet.contains(lastId)) {
						continue;
					}
					//token is selected as the first column
					startToken = row.getLong(0);
					
					//internal count 
					totalReadCount++;
					curReadCount++;
					//set next route key
					routeKey = getRouteKey(row);
					try {
						RowReaderTask<V> rr =  job.newTask();
						V ret = rr.process(row,rs.getColumnDefinitions(),rs.getExecutionInfo());
						job.processResult(ret);
					}
					catch (Exception e) {
						//will have been caught above
						logger.error(e.getMessage(), e);
						//bail out on client error
						throw new RuntimeException( e );
					}
				}
				 
				
				//exhausted the rs
				if (curRowCount < pageSize ) {
					logger.info("page size " + pageSize+ " exceeds row count " + curRowCount);
					//System.out.println("Count: " + count + " start: " + startId + " token: " + token);
					startToken++;
					//break;
				} 
				//CHECK THAT WE HAVE A COMPLETE OVER-LAP COMPARED TO LAST READ
				
				if (curReadCount == 0 && lastIdSet.containsAll(curIdSet)) {
					logger.warn("Wide row detected: pageSize must be greater than the widest CF row for optimal reading");
					readWide(row);
					startToken++;
				} else if (curReadCount == 0 ) {
					logger.info("No rows read at token {}" , startToken);
					startToken++;
				}
				lastIdSet = curIdSet;
				logger.debug("Total: {}  Cur Count: {} , startToken: {}", totalReadCount, curReadCount,startToken);
				
			}catch (ReadTimeoutException e) {
				logger.error(e.getMessage(),e);
				startToken++;
			}catch (QueryTimeoutException e) {
				logger.error(e.getMessage(),e);
				startToken++;
			}catch (NoHostAvailableException  e) {
				logger.error(e.getMessage(),e);
				//not much I can do with this
				throw e;
			}catch (Exception e) {
				logger.error("error during execution {}", cql);
				logger.error(e.getMessage(),e);
				throw e;
			}
		}
		logger.info("##Complete Read Total: {} ", totalReadCount);
		//done while more
		//job.onReadComplete();
		
	}
	//The pageSize is less than the size of the row, so we must 
	//resort to using paging via the cluster key
	//via assumes that the cluster key is ASC otherwise this logic may not work
	@SuppressWarnings("unchecked")
	protected void readWide(Row startRow) {
		
		Object partKey = get(startRow,config.getPkConfig().getPartitionKeys()[0]);
		//this will be modified as we iterate through the wide row
		Object clusterKey = get(startRow,config.getPkConfig().getClusterKeys()[0]);
		
		boolean more =true;
		//increment
		while (more) {
			String cql = generateWide(partKey, clusterKey, false,config.getPageSize());
			logger.debug("Excuting cql: {}", cql);
			ResultSet rs = session.execute(cql);
			List<Row> allRows = rs.all();
			if (allRows.size() == 0)
				more =false;
			else {
				for (Row row:allRows) {
					totalReadCount++;
					clusterKey = get(row,config.getPkConfig().getClusterKeys()[0]);
					try {
						RowReaderTask<V> rr = job.newTask();
						V result = rr.process(row,rs.getColumnDefinitions(),rs.getExecutionInfo());
						job.processResult(result);
					}catch (Exception e) {
						logger.error("error during execution {}", cql);
						//we will be changing how tasks are instantiated
						logger.error(e.getMessage(), e);
					}
					
				}
				logger.info("Total: {}, partKey: Cur ClusterKey: {} ", totalReadCount,partKey, clusterKey);
				
			}
			
			
		}
		
		
		
	}
	
	private ByteBuffer getRowCompositeKey(Row row) {
		ArrayList<Object> objList = new ArrayList<>();
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			objList.add(get(row,colinfo));			
		}
		for (ColumnInfo colinfo:config.getPkConfig().getClusterKeys()) {
			objList.add(get(row,colinfo));	
		}
		return Composite.toByteBuffer(objList);
		//return ret;
		
	}
	private ByteBuffer[] getRouteKey(Row row) {
		ArrayList<Object> objList = new ArrayList<>();
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			objList.add(get(row,colinfo));			
		}
		return CUtils.getBytesForRoute(objList);
		
	}
	public static Object get(Row row,ColumnInfo info) {
		Object ret = null;
		if (DataType.ascii().equals(info.type) )
			ret = row.getString(info.name);
		else if (DataType.text().equals(info.type))
			ret = row.getString(info.name);
		else if (DataType.bigint().equals(info.type))
			ret = row.getLong(info.name);
		else if (DataType.cdouble().equals(info.type))
			ret = row.getDouble(info.name);
		else if (DataType.timestamp().equals(info.type))
			ret = row.getDate(info.name);
		else if (DataType.cfloat().equals(info.type))
			ret = row.getFloat(info.name);
		else if (DataType.cint().equals(info.type))
			ret = row.getInt(info.name); 
		else if (DataType.uuid().equals(info.type))
			ret = row.getUUID(info.name); 
		else if (DataType.timeuuid().equals(info.type))
			ret = row.getUUID(info.name);
		return ret;
	}
	public static Object get(Row row, ColumnDefinitions.Definition colDefinition) {
		ColumnInfo colInfo = new ColumnInfo(colDefinition.getName(), colDefinition.getType());
		return get(row,colInfo);
	}
	private String getStringCompositeKey(Row row) {
		ByteBuffer bb = getCompositeKey(row);
		
		return new String(bb.array()).trim();
	}
	/**
	 * There is something weird here with the operation {@link Row#getBytesUnsafe(String)}
	 * Test this in the 2.0 driver.
	 * 
	 * 
	 * @param row
	 * @return
	 */
	private ByteBuffer getCompositeKey(Row row) {
		ArrayList<ByteBuffer> list = new ArrayList<>(config.getPkConfig().getPartitionKeys().length + config.getPkConfig().getClusterKeys().length);
		int balloc = 0;
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			ByteBuffer b = row.getBytesUnsafe(colinfo.name);
			balloc += b.capacity();
			list.add(b);
			
		}
		for (ColumnInfo colinfo:config.getPkConfig().getClusterKeys()) {
			ByteBuffer b = row.getBytesUnsafe(colinfo.name);
			balloc += b.capacity();
			list.add(b);
			
		}
		ByteBuffer ret = ByteBuffer.allocate(balloc);
		for (ByteBuffer bb:list) {
			ret.put(bb);
		}
		ret.rewind();
		logger.debug("getCompositeKey: <{}>" , ret);
		logger.debug("getCompositeKey: <{}>" , new String (ret.array()));
		return ret;
	}
	//partition key ids - support only one for now
	
	private String generateWide(Object ids,Object nextClusterKey,boolean inclusiveGT,int limit) {
		
		driver.em.SimpleStatement ss = new driver.em.SimpleStatement(prepareWide(inclusiveGT,limit), ids,nextClusterKey);
		
		return ss.buildQueryString();
	}
	private String prepareWide(boolean inclusiveGT,int limit) {
		StringBuilder builder = new StringBuilder("SELECT  " );
		builder.append(getTokenStr()).append(",");
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			builder.append(colinfo.name).append(",");
		}
		if (config.getPkConfig().getClusterKeys() != null)
			for (ColumnInfo colinfo:config.getPkConfig().getClusterKeys()) {
				builder.append(colinfo.name).append(",");
			}
		if (config.getOtherCols() != null)
			for (String other:config.getOtherCols()) {
				builder.append(other).append(",");
			}
		builder.replace(builder.length()-1, builder.length(), "");
		builder.append(" FROM " + config.getTable()+ " WHERE ");
		
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			builder.append(colinfo.name).append(" = ? AND ");
		}
		builder.append(config.getPkConfig().getClusterKeys()[0].getName());
		if (!inclusiveGT)
			builder.append(" > ?");
		else
			builder.append(" >= ?");
		builder.append(" limit " + limit);
		return builder.toString();
	}
	
	private String generateSelectPrefix(long startToken, long endToken) {
		StringBuilder builder = new StringBuilder("SELECT ");
		StringBuilder tokenPart = getTokenStr();
		builder.append(tokenPart).append(", ");
		
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			builder.append(colinfo.name).append(",");
		}
		if (config.getPkConfig().getClusterKeys() != null)
			for (ColumnInfo colinfo:config.getPkConfig().getClusterKeys()) {
				builder.append(colinfo.name).append(",");
			}
		if (config.getOtherCols() != null)
			for (String other:config.getOtherCols()) {
				builder.append(other).append(",");
			}
		
		builder.replace(builder.length()-1, builder.length(), "");
		builder.append(" FROM " + config.getTable()+ " ");
		
		//where
		builder.append(" WHERE " ).append(tokenPart).append(" >= " + startToken + " and " + tokenPart + " < " +endToken)
			.append(" limit " + config.getPageSize() );
		return builder.toString();
		
	}
	private StringBuilder getTokenStr() {
		StringBuilder tokenPart = new StringBuilder("token(");
		int i = 0;
		for (ColumnInfo colinfo:config.getPkConfig().getPartitionKeys()) {
			tokenPart.append(colinfo.name);
			if (i<config.getPkConfig().getPartitionKeys().length -1)
				tokenPart.append(", ");

		}
		tokenPart.append(") ");
		return tokenPart;
	}
	
	public Long getTotalReadCount() {
		return totalReadCount;
	}
}
