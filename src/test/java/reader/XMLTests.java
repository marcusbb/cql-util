package reader;

import static org.junit.Assert.*;

import java.io.File;

import javax.xml.bind.JAXBContext;

import migration.XMLConfig;

import org.apache.cassandra.db.ColumnIndex;
import org.junit.Test;

import com.datastax.driver.core.DataType;

import driver.em.CassConfig;

public class XMLTests {

	@Test
	public void generateXML() throws Exception {
		
		JAXBContext jc = JAXBContext.newInstance(XMLConfigFactory.class);
		File fout = new File("reader-config-generated.xml");
		
		ReaderConfig readerConfig = new ReaderConfig();
		readerConfig.setCassConfig(new CassConfig());
		PKConfig pkconfig = new PKConfig();
		
		readerConfig.setPkConfig(
				new PKConfig(
				new ColumnInfo[]{new ColumnInfo("id", DataType.ascii())}, 
				new ColumnInfo[]{ new ColumnInfo("name",DataType.ascii())}
				));
		jc.createMarshaller().marshal(readerConfig, fout);
		
	}

}
