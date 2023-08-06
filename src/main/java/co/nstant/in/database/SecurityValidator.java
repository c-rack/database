package co.nstant.in.database;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

class SecurityValidator {

	private Config config;
	private boolean readOnly = false;

	public SecurityValidator() {
		this(SqlConformanceEnum.DEFAULT);
	}

	public SecurityValidator(SqlConformanceEnum conformance) {
		config = SqlParser.config().withCaseSensitive(false).withConformance(conformance);
	}

	public SecurityValidator readOnly() {
		this.readOnly = true;
		return this;
	}

	public void validate(String sql) {
		try {
			SqlParser sqlParser = SqlParser.create(sql, config);
			SqlNodeList list = sqlParser.parseStmtList();
			SqlNode sqlNode = list.get(0);
			ensureSingleStatement(list);
			ensureMethodIsAllowed(sqlNode.getKind());
			sqlNode.accept(new SecuritySqlVisitor());
		} catch (SecurityException securityException) {
			throw securityException;
		} catch (Exception exception) {
			throw new SecurityException("validate()", exception);
		}
	}

	private void ensureSingleStatement(SqlNodeList list) {
		if (list.size() > 1) {
			throw new SecurityException("Multiple SQL statements are not allowed");
		}
	}

	private void ensureMethodIsAllowed(SqlKind sqlKind) {
		switch (sqlKind) {
		case SELECT:
			break;
		case INSERT:
		case UPDATE:
		case DELETE:
			if (readOnly) {
				throw new SecurityException("Connection is read-only");
			}
			break;
		default:
			throw new SecurityException("SQL command '" + sqlKind + "' is not allowed");
		}
	}

}
