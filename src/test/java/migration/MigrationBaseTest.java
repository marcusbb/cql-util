package migration;

import java.sql.Connection;
import java.sql.DriverManager;

public class MigrationBaseTest {

	public static void beforeClass() {

		try {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			Connection con = DriverManager
					.getConnection("jdbc:derby:memory:testDB;create=true");

			con.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
