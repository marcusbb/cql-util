package reader;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.spi.LoggerFactory;
import org.slf4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import driver.em.CUtils;
import driver.em.CassConfig;

public class CQLRowReaderImproved {

	private Cluster cluster;
	
	private Session session;
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(CQLRowReaderImproved.class);
	
	ReaderConfig config;
	
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
		reader.session = reader.cluster.connect("icrs");
		
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
		
		//CQL Row count
		long count = 0;
		
		
		//HashSet container to keep track of all of items in the last fetch
		//it's compared to a current page count to exclude duplicates from previous
		//query
		HashSet<ByteBuffer> lastIdSet = new HashSet<ByteBuffer>(pageSize);
		
		while (more) {
			
			
			SimpleStatement ss = new SimpleStatement( generateSelectPrefix(startToken,endToken)  );
			
			ResultSet rs = session.execute(ss);


			Iterator<Row> iter = rs.iterator();
			
			//bail on empty result set
			if (!iter.hasNext())
				break;
			//hold the last row id (composite key)
			ByteBuffer lastId = null;
			
			Row row = null;
			int curRowCount = 0;
			HashSet<ByteBuffer> curIdSet = new HashSet<ByteBuffer>(pageSize);
			
			//THROUGH THE ROW RESULT - per start token
			while (iter.hasNext()) {
				curRowCount++;
				row = iter.next();
				
				lastId = getCompositeKey(row);
				
				curIdSet.add(lastId);
				if (lastIdSet.contains(lastId)) {
					continue;
				}
				
				startToken = row.getLong(2);
				
				//internal count 
				count++;
				
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
			
			logger.info("Final Count: " + count + " token: " + startToken);
		}
		
	}
	//ByteBuffer simply packed together in order
	//token part + non-token part
	private ByteBuffer getCompositeKey(Row row) {
		ArrayList<ByteBuffer> list = new ArrayList<>(config.getPkConfig().getTokenPart().length + config.getPkConfig().getNonTokenPart().length);
		int balloc = 0;
		for (ColumnInfo colinfo:config.getPkConfig().getTokenPart()) {
			ByteBuffer b = row.getBytesUnsafe(colinfo.name);
			balloc += b.array().length;
			list.add(b);
			
		}
		for (ColumnInfo colinfo:config.getPkConfig().getNonTokenPart()) {
			ByteBuffer b = row.getBytesUnsafe(colinfo.name);
			balloc += b.array().length;
			list.add(b);
			
		}
		ByteBuffer ret = ByteBuffer.allocate(balloc);
		for (ByteBuffer bb:list) {
			ret.put(bb);
		}
		return ret;
	}
	
	private String generateSelectPrefix(long startToken, long endToken) {
		StringBuilder builder = new StringBuilder("SELECT ");
		for (ColumnInfo colinfo:config.getPkConfig().getTokenPart()) {
			builder.append(colinfo.name).append(",");
		}
		for (ColumnInfo colinfo:config.getPkConfig().getNonTokenPart()) {
			builder.append(colinfo.name).append(",");
		}
		//a limitation in token function - only accepts single argument - to doubly confirm
		String tokenPart = "token(" + config.getPkConfig().getTokenPart()[0].name + ") ";
		builder.append(tokenPart);
		builder.append(" FROM " + config.getTable()+ " ");
		
		//where
		builder.append(" WHERE " ).append(tokenPart).append(" >= " + startToken + " and " + tokenPart + " < " +endToken)
			.append(" limit " + config.getPageSize() );
		return builder.toString();
		
	}
}
