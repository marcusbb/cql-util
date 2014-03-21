package reader.samples;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import reader.CQLRowReader;
import reader.MTJobBootStrap;
import reader.ReaderConfig;
import reader.ReaderJob;
import reader.RowReaderTask;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

/**
 * 
 * Sample to generate a CSV report.
 * 
 * 
 *
 */
public class CSVReportJob extends ReaderJob<Void> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2813562426255630201L;
	final FileOutputStream fout;
	final String delim;
	static byte[] newLine = "\n".getBytes();
	final static Object fileWriteLock = new Object();
	
	public CSVReportJob(File dest,String delim) throws IOException {
		this.fout = new FileOutputStream(dest);
		this.delim = delim;
	}
	@Override
	public RowReaderTask<Void> newTask() throws Exception {
		return new RowReaderTask<Void>() {
			
			@Override
			public Void process(Row row, ColumnDefinitions colDefs,
					ExecutionInfo execInfo) {
				try {
					
					synchronized(fileWriteLock) {
						for (ColumnDefinitions.Definition def:colDefs) {
							Object obj = CQLRowReader.get(row, def);
							if (obj != null) {
								fout.write(obj.toString().getBytes());
							}
							fout.write(delim.getBytes());
						}
						fout.write(newLine);
					}
				}catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				return null;
			}
		};
	}

	@Override
	public void processResult(Void result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onReadComplete() {
		// TODO Auto-generated method stub
		
	}

	public static class Main extends MTJobBootStrap {
		
		final File dest;
		final static char[] delimch = {0x7f};
		final static String delim = new String(delimch);
		
		public Main(int nThreads,File dest,String delim) {
			super(nThreads);
			this.dest = dest;
			
			
		}

		public static void main(String []args) {
			//System.setProperty("config", "perf-reader-config.xml");
			File f = new File("output.csv");
			Main boot = new Main(10,f,delim);
			boot.bootstrap();
			boot.runJob();
			
		}

		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig)  {
		try {
			return new CSVReportJob(dest,delim);
		}catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		}
	}
	

}
