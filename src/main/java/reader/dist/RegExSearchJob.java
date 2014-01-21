package reader.dist;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.Row;

import driver.em.Composite;

import reader.ReaderConfig;
import reader.RowReaderTask;

public class RegExSearchJob extends DistReaderJob<Composite> {

	private final Pattern pattern;
	private final String colName;
	private final ReaderConfig readerConfig;
	
	
	public RegExSearchJob(String exp,String colName,ReaderConfig readerConfig) {
		this.pattern = Pattern.compile(exp);
		this.colName = colName;
		this.readerConfig = readerConfig;
	}
	
	public static class Callback extends CompletionCallback {

		public Callback() {
			
		}
		@Override
		public void onResponse(ReaderResult readerResult) {
			
		}
		
		
	}
	@Override
	public CompletionCallback newCompletionCallBack() {
		return new Callback();
	}

	
	public static class Finder implements RowReaderTask<Composite> {

		private final Pattern pattern;
		private final String colName;
		private final ReaderConfig config;
		
				
		public Finder(Pattern pattern,String colName,ReaderConfig config) {
			this.pattern = pattern;
			this.colName = colName;
			this.config = config;
			
		}
		@Override
		public Composite process(Row row) {
			
			if (pattern.matcher(row.getString(colName)).matches() ) {
				ArrayList<Object> compositeRow = new ArrayList<>();
				for (int i=0;i<config.getPkConfig().getTokenPart().length;i++) {
					//first part of the partition key or we'll error out
					compositeRow.add(row.getString(config.getPkConfig().getTokenPart()[0].getName()));
					
				}
				
			}
			return null;
		}
		
	}
	
	
	
	@Override
	public RowReaderTask<Composite> newTask() throws Exception {
		return new Finder(this.pattern, this.colName,this.readerConfig);
		
	}



	@Override
	public void processResult(Composite result) {
		
		
	}
	

	

}
