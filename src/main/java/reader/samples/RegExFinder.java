package reader.samples;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

import reader.JobBootStrap;
import reader.ReaderConfig;
import reader.ReaderJob;
import reader.RowReaderTask;

/**
 * 
 * Find via a reg expression and build a report
 *
 */
public class RegExFinder extends ReaderJob<RegExFinder.FormattedReport> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -753063495008105310L;

	//some fabricated report format
	public static class FormattedReport {
		
	}
	
	//the actual task to find
	public static class FindTask implements RowReaderTask<FormattedReport> {

		@Override
		public FormattedReport process(Row row,ColumnDefinitions colDef,ExecutionInfo execInfo) {
			
			//find it in the row
			
			//return report
			return new FormattedReport();
		}
		
	}
	@Override
	public RowReaderTask<RegExFinder.FormattedReport> newTask() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public void processResult(RegExFinder.FormattedReport result) {
		
		//process the result of the report
	}
	
	//bootstrap
	public static class RegExMain extends JobBootStrap {

		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig) {

			return new RegExFinder();
		}
		
	}
	@Override
	public void onReadComplete() {
		// TODO Auto-generated method stub
		
	}

}
