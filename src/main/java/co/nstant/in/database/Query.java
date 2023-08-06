package co.nstant.in.database;

import java.util.List;

public class Query implements AutoCloseable {

	private Database database;
	private String sql;
	private org.sql2o.Query sql2oQuery;

	protected Query(Database database, String sql, org.sql2o.Query sql2oQuery) {
		this.database = database;
		this.sql = sql;
		this.sql2oQuery = sql2oQuery;
	}

	public Query addParameter(String name, Boolean value) {
		sql2oQuery.addParameter(name, value);
		return this;
	}

	public Query addParameter(String name, Integer value) {
		sql2oQuery.addParameter(name, value);
		return this;
	}

	public Query addParameter(String name, Long value) {
		sql2oQuery.addParameter(name, value);
		return this;
	}

	public Query addParameter(String name, String value) {
		sql2oQuery.addParameter(name, value);
		return this;
	}

	public Query bind(Object object) {
		sql2oQuery.bind(object);
		return this;
	}

	@Override
	public void close() {
		sql2oQuery.close();
	}

	public <T> List<T> executeAndFetch(Class<T> returnType) {
		if (database.hasSlowQueryCallback()) {
			return executeAndFetchWithSlowQueryCheck(returnType);
		} else {
			return sql2oQuery.executeAndFetch(returnType);
		}
	}

	private <T> List<T> executeAndFetchWithSlowQueryCheck(Class<T> returnType) {
		long t0 = System.nanoTime();
		List<T> result = sql2oQuery.executeAndFetch(returnType);
		long t1 = System.nanoTime();
		long duration = t1 - t0;
		if (duration >= database.getSlowQueryNanoseconds()) {
			database.onSlowQuery(sql, duration);
		}
		return result;
	}

	public <T> T executeAndFetchFirst(Class<T> returnType) {
		if (database.hasSlowQueryCallback()) {
			return executeAndFetchFirstWithSlowQueryCheck(returnType);
		} else {
			return sql2oQuery.executeAndFetchFirst(returnType);
		}
	}

	private <T> T executeAndFetchFirstWithSlowQueryCheck(Class<T> returnType) {
		long t0 = System.nanoTime();
		T result = sql2oQuery.executeAndFetchFirst(returnType);
		long t1 = System.nanoTime();
		long duration = t1 - t0;
		if (duration >= database.getSlowQueryNanoseconds()) {
			database.onSlowQuery(sql, duration);
		}
		return result;
	}

	public Connection executeUpdate() {
		if (database.hasSlowQueryCallback()) {
			return executeUpdateWithSlowQueryCheck();
		} else {
			return new Connection(database, sql2oQuery.executeUpdate());
		}
	}

	private Connection executeUpdateWithSlowQueryCheck() {
		long t0 = System.nanoTime();
		Connection result = new Connection(database, sql2oQuery.executeUpdate());
		long t1 = System.nanoTime();
		long duration = t1 - t0;
		if (duration >= database.getSlowQueryNanoseconds()) {
			database.onSlowQuery(sql, duration);
		}
		return result;
	}

}
