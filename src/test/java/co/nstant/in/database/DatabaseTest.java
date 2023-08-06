package co.nstant.in.database;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DatabaseTest {

	private final static String JDBC = "jdbc:hsqldb:mem:database";
	private final static String USER = "SA";
	private final static String PASS = "";
	
	@Test
	public void testConnection() {
		boolean databaseOpened = false;
		Database database = new Database(JDBC, USER, PASS);
		try(Connection connection = database.open()) {
			databaseOpened = true;
		}
		assertTrue(databaseOpened);
	}
	
}
