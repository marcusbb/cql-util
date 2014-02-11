package reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

//going to dump/refactor this class
public class LargeRowsTask implements RowReaderTask<Object>{

	
	static Logger logger = LoggerFactory.getLogger(LargeRowsTask.class);
	final ColumnInfo colToCount;
	final int threshold;
	public LargeRowsTask(ColumnInfo colToCount,int threshold) {
		this.colToCount = colToCount;
		this.threshold = threshold;
	}
	@Override
	public Object process(Row row) {
		
		Object colObj = CQLRowReader.get(row, colToCount);//row.getString(colToCount);
		
		
		return colObj;
		
		
	}

	
	
	

}
