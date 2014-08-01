package util;

import junit.framework.Assert;
import migration.XMLConfig;
import org.junit.Test;
import util.JAXBUtil;

public class JAXBUtilTest {
	
	@Test
	public void testVarSubstitution() throws Exception{
		String jdbcUrl = "jdbc:oracle:thin:@dbprv59yyz.labyyz.testnet.rim.net:1523:Vostok";
		String dbUser = "DV_ICRS15";
		String dbPassword = "cardhu";
		String cassandraHost = "cass-001-vmyyz.labyyz.testnet.rim.net";
		String cassandraLocalDC = "DC1";
		int cassandraNativePort = 1523;
		String cassandraUser = "cassandra";
		String cassandraPassword = "cassandra";
				
		System.setProperty("migration.jdbc.url", jdbcUrl);
		System.setProperty("migration.db.user", dbUser);
		System.setProperty("migration.db.password", dbPassword);
		System.setProperty("migration.cassandra.host", cassandraHost);
		System.setProperty("migration.cassandra.local.dc", cassandraLocalDC);
		System.setProperty("migration.cassandra.native.port", String.valueOf(cassandraNativePort));
		System.setProperty("migration.cassandra.user", cassandraUser);
		System.setProperty("migration.cassandra.password", cassandraPassword);
		
		String fileName = "migration/var-substitute-mapping.xml";
	  
	    XMLConfig config = (XMLConfig)JAXBUtil.unmarshalXmlFile(fileName, XMLConfig.class);
	    Assert.assertNotNull(config);
	    Assert.assertEquals(jdbcUrl, config.getJdbcUrl());
	    Assert.assertEquals(dbUser, config.getJdbcUsername());
	    Assert.assertEquals(dbPassword, config.getJdbcPassword());
	    Assert.assertEquals(cassandraHost, config.getCassConfig().getContactHostsName()[0]);
	    Assert.assertEquals(cassandraNativePort, config.getCassConfig().getNativePort());
	    Assert.assertEquals(cassandraLocalDC, config.getCassConfig().getLocalDataCenterName());
	    Assert.assertEquals(cassandraUser, config.getCassConfig().getUsername());
	    Assert.assertEquals(cassandraPassword, config.getCassConfig().getPassword());
	}
}
