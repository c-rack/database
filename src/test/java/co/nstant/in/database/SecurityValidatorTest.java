package co.nstant.in.database;

import org.junit.Test;

public class SecurityValidatorTest {

	@Test(expected = SecurityException.class)
	public void testEmptyStatementsAreDisallowed() {
		new SecurityValidator().validate("");
	}

	@Test(expected = SecurityException.class)
	public void testMultipleStatementsAreDisallowed() {
		new SecurityValidator().validate("UPDATE t SET c = 1; SELECT c FROM t");
	}

	@Test(expected = SecurityException.class)
	public void testSleepIsDisallowed() {
		new SecurityValidator().validate("SELECT * FROM t WHERE c LIKE '%nomatch' OR SLEEP(300) AND '1%'");
	}

	@Test(expected = SecurityException.class)
	public void testSleepIsDisallowedLowercased() {
		new SecurityValidator().validate("SELECT * FROM t WHERE c LIKE '%nomatch' OR sleep(300) AND '1%'");
	}

	@Test(expected = SecurityException.class)
	public void testBenchmarkIsDisallowed() {
		new SecurityValidator().validate("SELECT BENCHMARK(1000000,MD5(1))");
	}

	@Test(expected = SecurityException.class)
	public void testUserIsDisallowed() {
		new SecurityValidator().validate("SELECT USER()");
	}

	@Test(expected = SecurityException.class)
	public void testVersionIsDisallowed() {
		new SecurityValidator().validate("SELECT VERSION()");
	}

	@Test(expected = SecurityException.class)
	public void testSchemaIsDisallowed() {
		new SecurityValidator().validate("SELECT SCHEMA()");
	}

	@Test(expected = SecurityException.class)
	public void testDropTableIsDisallowed() {
		new SecurityValidator().validate("DROP users");
	}

	@Test(expected = SecurityException.class)
	public void testTruncateTableIsDisallowed() {
		new SecurityValidator().validate("TRUNCATE users");
	}

}
