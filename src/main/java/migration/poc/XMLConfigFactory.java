package migration.poc;

import javax.xml.bind.annotation.XmlRegistry;

@XmlRegistry
public class XMLConfigFactory {


	public XMLConfig createConfiguration() {
		return new XMLConfig();
	}
	
}
