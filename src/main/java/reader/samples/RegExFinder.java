package reader.samples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import reader.CQLRowReader;
import reader.MTJobBootStrap;
import reader.PKConfig.ColumnInfo;
import reader.ReaderConfig;
import reader.ReaderJob;
import reader.RowReaderTask;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

import driver.em.CassConfig;

/**
 * 
 * Find via a reg expression and build a report
 *
 * This sample builds the {@link ReaderConfig} manually.
 * 
 * 
 * 
 */
public class RegExFinder extends ReaderJob<RegExFinder.FormattedReport> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -753063495008105310L;
	private static ColumnInfo []colsToSearch;
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
	//single threaded
	public static class RegExMain extends MTJobBootStrap {

		public RegExMain() {
			 
		}
		public static void main(String []args) {
			
			
			//
			RegExMain main = new RegExMain();
			colsToSearch = new ColumnInfo[]{new ColumnInfo("value",DataType.text())};
			//build the reader config
			ReaderConfig config = new ReaderConfig();
			CassConfig cassConfig = new CassConfig();
			cassConfig.setContactHostsName(new String[]{"marcus-v4.rim.net"});
			config.setCassConfig(cassConfig);
			config.setTable("device_advertised_services");
			config.setKeyspace("icrs");
			config.setOtherCols(new String[]{"value"});
			
			main.bootstrap(config);
			
			main.runJob();
			
		}
		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig) {

			return new RegExFinder();
		}
		
	}
	
	@Override
	public void onReadComplete() {
		System.out.println("My job is done");
		
	}

}
