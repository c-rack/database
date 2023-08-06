package co.nstant.in.database;

public interface SlowQueryCallback {

	public void onSlowQuery(String sql, long nanoseconds);

}
