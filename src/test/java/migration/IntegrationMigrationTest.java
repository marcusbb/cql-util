package migration;

import org.junit.Ignore;
import org.junit.Test;

public class IntegrationMigrationTest {

	//This is a full end 2 end test, not to be run as part of junit tests
	@Ignore
	@Test
	public void testMigrationEnd2End() throws Exception {
	  
		String fileName = "migration/mapping.xml";
	    Main main = new Main(fileName);
		main.execute();
	}
}
