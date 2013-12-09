package reader;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import driver.em.CUtils;
import driver.em.CassConfig;
import driver.em.Composite;

public class CQLRowReaderImproved {

	protected Cluster cluster;
	
	protected Session session;
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(CQLRowReaderImproved.class);
	
	ReaderConfig config;
	
	long totalReadCount = 0;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		
				
		CQLRowReaderImproved reader = new CQLRowReaderImproved();
		
		JAXBContext jc = JAXBContext.newInstance(ReaderConfig.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("reader-config.xml");
		
		reader.config = (ReaderConfig)unmarshaller.unmarshal(ins);
		
		reader.cluster = CUtils.createCluster(new CassConfig());
		reader.session = reader.cluster.connect(reader.config.getKeyspace());
		
		//for exception safety below
		Class.forName( reader.config.getReaderTask() ).newInstance();
		
		reader.read();
		
		System.exit(0);
	}
	
	public void read() {
		boolean more = true;
		
		//start at the beginning of the token range.
		Long startToken = config.getStartToken();
		
		Long endToken = config.getEndToken();
		
		
		int pageSize = config.getPageSize();
		
			
		
		//HashSet container to keep track of all of items in the last fetch
		//it's compared to a current page count to exclude duplicates from previous
		//query
		HashSet<Object> lastIdSet = new HashSet<Object>(pageSize);
		
		while (more) {
			
			String cql = generateSelectPrefix(startToken,endToken) ;
			SimpleStatement ss = new SimpleStatement( cql  );
			ByteBuffer[] routeKey = CUtils.getBytesForRoute(startToken);
			ss.setRoutingKey(routeKey);
			logger.info("Executing cql: {} , routeKey: {} " ,cql, startToken);
			
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
				
				try {
					RowReaderTask rr = (RowReaderTask)Class.forName( config.getReaderTask() ).newInstance();
					rr.process(row);
				}catch (Exception e) {
					//will have been caught above
					logger.error(e.getMessage(), e);
				}
			}
			lastIdSet = curIdSet;
			//exhausted the rs
			if (curRowCount < pageSize ) {
				logger.info("page size " + pageSize+ " exceeds row count " + curRowCount);
				//System.out.println("Count: " + count + " start: " + startId + " token: " + token);
				startToken++;
				//break;
			} 
			if (curReadCount == 0) {
				logger.info("No rows read at token {}" , startToken);
				startToken++;
			}
			
			logger.info("Total: {}  Cur Count: {} , startToken: {}", totalReadCount, curReadCount,startToken);
		}
		
	}
	
	private ByteBuffer getRowCompositeKey(Row row) {
		ArrayList<Object> objList = new ArrayList<>();
		for (ColumnInfo colinfo:config.getPkConfig().getTokenPart()) {
			objList.add(get(row,colinfo));			
		}
		for (ColumnInfo colinfo:config.getPkConfig().getNonTokenPart()) {
			objList.add(get(row,colinfo));	
		}
		return Composite.toByteBuffer(objList);
		//return ret;
		
	}
	private Object get(Row row,ColumnInfo info) {
		Object ret = null;
		if (DataType.ascii().equals(info.type) )
			ret = row.getString(info.name);
		else if (DataType.bigint().equals(info.type))
			ret = row.getLong(info.name);
		else if (DataType.cdouble().equals(info.type))
			ret = row.getDouble(info.name);
		return ret;
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
		ArrayList<ByteBuffer> list = new ArrayList<>(config.getPkConfig().getTokenPart().length + config.getPkConfig().getNonTokenPart().length);
		int balloc = 0;
		for (ColumnInfo colinfo:config.getPkConfig().getTokenPart()) {
			ByteBuffer b = row.getBytesUnsafe(colinfo.name);
			balloc += b.capacity();
			list.add(b);
			
		}
		for (ColumnInfo colinfo:config.getPkConfig().getNonTokenPart()) {
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
	
	private String generateSelectPrefix(long startToken, long endToken) {
		StringBuilder builder = new StringBuilder("SELECT ");
		//a limitation in token function - only accepts single argument - to doubly confirm
		String tokenPart = "token(" + config.getPkConfig().getTokenPart()[0].name + ") ";
		builder.append(tokenPart).append(",");
		for (ColumnInfo colinfo:config.getPkConfig().getTokenPart()) {
			builder.append(colinfo.name).append(",");
		}
		for (ColumnInfo colinfo:config.getPkConfig().getNonTokenPart()) {
			builder.append(colinfo.name).append(",");
		}
		builder.replace(builder.length()-1, builder.length(), "");
		builder.append(" FROM " + config.getTable()+ " ");
		
		//where
		builder.append(" WHERE " ).append(tokenPart).append(" >= " + startToken + " and " + tokenPart + " < " +endToken)
			.append(" limit " + config.getPageSize() );
		return builder.toString();
		
	}
	
	public Long getTotalReadCount() {
		return totalReadCount;
	}
}
