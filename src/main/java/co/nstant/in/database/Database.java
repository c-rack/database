package co.nstant.in.database;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Sql2o;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Database {

	private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);
	public static final long NO_SLOW_QUERY = -1;

	private static Map<String, Object> directAccessMethods = new ConcurrentHashMap<>();

	private HikariDataSource hikariDataSource;
	private Sql2o sql2o;
	private SqlConformanceEnum conformance = SqlConformanceEnum.DEFAULT;
	private long slowQueryNanoseconds = NO_SLOW_QUERY;
	private HashSet<SlowQueryCallback> slowQueryCallbacks = new HashSet<>();

	public Database(String jdbcUrl, String username, String password) {
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(jdbcUrl);
		hikariConfig.setUsername(username);
		hikariConfig.setPassword(password);
		hikariDataSource = new HikariDataSource(hikariConfig);
		sql2o = new Sql2o(hikariDataSource);
	}

	public Database(Database database, Sql2o sql2o) {
		this.hikariDataSource = database.hikariDataSource;
		this.conformance = database.conformance;
		this.slowQueryNanoseconds = database.slowQueryNanoseconds;
		this.slowQueryCallbacks = database.slowQueryCallbacks;
		this.sql2o = sql2o;
	}

	public Connection open() {
		return new Connection(this, sql2o.open());
	}

	public Connection startTransaction() {
		return new Connection(this, sql2o.beginTransaction());
	}

	public Database withConnectionTimeout(long connectionTimeoutMs) {
		LOGGER.debug("withConnectionTimeout({}ms)", connectionTimeoutMs);
		hikariDataSource.setConnectionTimeout(connectionTimeoutMs);
		return this;
	}

	public Database withIdleTimeout(long idleTimeoutMs) {
		LOGGER.debug("withIdleTimeout({}ms)", idleTimeoutMs);
		hikariDataSource.setIdleTimeout(idleTimeoutMs);
		return this;
	}

	public Database withMinimumIdleConnections(int minIdleConnections) {
		LOGGER.debug("withMinimumIdleConnections({})", minIdleConnections);
		hikariDataSource.setMinimumIdle(minIdleConnections);
		return this;
	}

	public Database withMaximumPoolSize(int maximumPoolSize) {
		LOGGER.debug("withMaximumPoolSize({})", maximumPoolSize);
		hikariDataSource.setMaximumPoolSize(maximumPoolSize);
		return this;
	}

	public Database withConformance(SqlConformanceEnum conformance) {
		LOGGER.debug("withConformance({})", conformance);
		this.conformance = conformance;
		return this;
	}

	public SqlConformanceEnum getConformance() {
		return conformance;
	}

	public Database withSlowQueryMilliseconds(long slowQueryMilliseconds) {
		return withSlowQueryNanoseconds(slowQueryMilliseconds * 1_000_000L);
	}

	public Database withSlowQueryNanoseconds(long slowQueryNanoseconds) {
		LOGGER.debug("withSlowQueryNanoseconds({}ns)", slowQueryNanoseconds);
		this.slowQueryNanoseconds = slowQueryNanoseconds;
		return this;
	}

	public long getSlowQueryNanoseconds() {
		return slowQueryNanoseconds;
	}

	boolean hasSlowQueryCallback() {
		return slowQueryNanoseconds >= 0 && !slowQueryCallbacks.isEmpty();
	}

	public void addSlowQueryCallback(SlowQueryCallback slowQueryCallback) {
		slowQueryCallbacks.add(slowQueryCallback);
	}

	public HikariDataSource hikariDataSource() {
		logDirectAccess("hikariDataSource()");
		return hikariDataSource;
	}

	public Sql2o sql2o() {
		logDirectAccess("sql2o()");
		return sql2o;
	}

	void onSlowQuery(String sql, long nanoseconds) {
		new Thread("onSlowQuery") {

			@Override
			public void run() {
				LOGGER.debug("onSlowQuery('{}', {}ns)", sql, nanoseconds);

				for (SlowQueryCallback slowQueryCallback : slowQueryCallbacks) {
					executeSlowQueryCallback(slowQueryCallback, sql, nanoseconds);
				}
			}

			private void executeSlowQueryCallback(SlowQueryCallback slowQueryCallback, String sql, long nanoseconds) {
				try {
					slowQueryCallback.onSlowQuery(sql, nanoseconds);
				} catch (Exception exception) {
					LOGGER.warn("executeSlowQueryCallback()", exception);
				}
			};

		}.start();
	}

	private void logDirectAccess(String method) {
		if (!directAccessMethods.containsKey(method)) {
			directAccessMethods.put(method, null);
			LOGGER.warn("Direct access to {}.", method);
			LOGGER.warn("Please open an issue why this was necessary, thanks.");
		}
	}

}
