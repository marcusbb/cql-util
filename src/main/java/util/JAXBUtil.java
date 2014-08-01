package util;

import java.io.File;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.lang3.text.StrSubstitutor;



public class JAXBUtil {
	public static Object unmarshalXmlFile(String fileName, Class jaxbClass) throws Exception{
		InputStream ins = null;
		File file = new File(fileName);
		if(file.exists()){
			ins = file.toURI().toURL().openStream();
		}else{
			ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
		}
		
		JAXBContext jc = JAXBContext.newInstance(jaxbClass);
		XMLInputFactory xif = XMLInputFactory.newFactory();
        StreamSource source = new StreamSource(ins);
        XMLStreamReader xsr = xif.createXMLStreamReader(source);
        xsr = new StreamReaderDelegate(xsr) {
        	@Override
			public char[] getTextCharacters() {
				return getText().toCharArray();
			}

			@Override
            public String getText() {
				return StrSubstitutor.replaceSystemProperties(super.getText());
            }

	        @Override
	        public int getTextLength() {
	            return getText().length();
	        }

	        @Override
	        public int getTextStart() {
	            return 0;
	        }
        };
        
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		return unmarshaller.unmarshal(xsr);
	}
}
