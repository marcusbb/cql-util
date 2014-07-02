package reader.samples;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

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
	static String newLine = "\n";
	static byte[] newLineb = "\n".getBytes();
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
					StringBuilder builder = new StringBuilder();
					
					for (ColumnDefinitions.Definition def:colDefs) {
						Object obj = CQLRowReader.get(row, def);
						if (obj != null) {
							//System.out.print("col: " +obj );
							builder.append(obj.toString());
						}
						builder.append(delim);
					}
					builder.append(newLine);
					fout.write(builder.toString().getBytes());
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
		final static String delim = "\t";//new String(delimch);
		String delimiter = null;
		
		public Main(int nThreads,File dest,String delim) {
			super(nThreads);
			this.dest = dest;
			this.delimiter = delim;
			
			
		}

		public static void main(String []args) {
			//System.setProperty("config", "perf-reader-config.xml");
			try {
				int threads = 1;
				String fileName = "output.csv";
				String delimiter = delim;
				
				if (args.length >= 1)
					fileName = args[0];
				if (args.length >= 2)
					threads = Integer.parseInt(args[1]);
				if (args.length >= 3)
					delimiter = args[2];
				
				File f = new File(fileName);
				Main boot = new Main(threads,f,delimiter);
				//provide the input stream
				InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("reader/samples/csv-config.xml");
				boot.bootstrap(null,ins);
				boot.runJob();
			}catch (Exception e) {
				e.printStackTrace();
				usage();
			}
			
		}
		private static void usage() {
			System.out.println(String.format("java %s [threads] [outputFile] [delimiter]",Main.class.getName()));
		}

		@Override
		public ReaderJob<?> initJob(ReaderConfig readerConfig)  {
		try {
			return new CSVReportJob(dest,delimiter);
		}catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		}
	}
	

}
