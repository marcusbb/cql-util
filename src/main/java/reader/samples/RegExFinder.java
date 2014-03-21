package reader.samples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

import reader.CQLRowReader;
import reader.JobBootStrap;
import reader.PKConfig.ColumnInfo;
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
	private ColumnInfo []colsToSearch;
	private Pattern pattern = Pattern.compile("[a-b].*");
	
	private Collection<FormattedReport> collectedLines = Collections.synchronizedList(new ArrayList<FormattedReport>());
	
	//some fabricated report format
	public static class FormattedReport {
		public FormattedReport(String line) {
			
		}
		String line;
	}
	
	//the actual task to find
	public class FindTask implements RowReaderTask<FormattedReport> {
		
		@Override
		public FormattedReport process(Row row,ColumnDefinitions colDef,ExecutionInfo execInfo) {
			
			String str = null;
			//find it in the row
			for (int i=0;i<colsToSearch.length;i++) {
				String strTocomp = CQLRowReader.get(row,colsToSearch[i]).toString();
				pattern.matcher( strTocomp ).matches();
				str = strTocomp;
			}
			//return report
			return new FormattedReport(str);
		}
		
	}
	@Override
	public RowReaderTask<RegExFinder.FormattedReport> newTask() throws Exception {
		return new FindTask(); 
	}

	
	@Override
	public void processResult(RegExFinder.FormattedReport result) {
		if (result.line != null)
			collectedLines.add(result);
	}
	
	//bootstrap
	public static class RegExMain extends JobBootStrap {

		public static void main(String []args) {
			
			int arg_len = args.length;
			
			//
		}
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
