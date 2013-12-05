package reader;

import javax.xml.bind.annotation.XmlRegistry;

@XmlRegistry
public class XMLConfigFactory {


	public ReaderConfig createConfiguration() {
		return new ReaderConfig();
	}
	
}
