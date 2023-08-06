package co.nstant.in.database;

public class Connection implements AutoCloseable {

	private org.sql2o.Connection connection;
	private SecurityValidator securityValidator;
	private Database database;
	private boolean secure = true;

	public Connection(Database database, org.sql2o.Connection sql2oConnection) {
		this.database = database;
		this.connection = sql2oConnection;
		this.securityValidator = new SecurityValidator(database.getConformance());
	}

	public Connection insecure() {
		secure = false;
		return this;
	}

	public Connection secure() {
		secure = true;
		return this;
	}

	public Connection readOnly() {
		securityValidator.readOnly();
		return this;
	}

	public Database commit() {
		return new Database(database, connection.commit());
	}

	public Query query(String sql) {
		if (secure) {
			securityValidator.validate(sql);
		}
		return new Query(database, sql, connection.createQuery(sql, true));
	}

	@Override
	public void close() {
		connection.close();
	}

	public int getResult() {
		return connection.getResult();
	}

}
