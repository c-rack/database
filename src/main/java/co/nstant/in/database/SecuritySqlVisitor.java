package co.nstant.in.database;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

class SecuritySqlVisitor extends SqlShuttle {

	@Override
	public SqlNode visit(SqlCall call) {
		if (call.getKind() == SqlKind.OTHER_FUNCTION) {
			String functionName = call.getOperator().getName().toUpperCase();
			switch (functionName) {
			case "BENCHMARK":
			case "SCHEMA":
			case "SLEEP":
			case "USER":
			case "VERSION":
				throw new SecurityException("Function '" + functionName + "' is not allowed");
			default:
				break;
			}
		}

		for (SqlNode node : call.getOperandList()) {
			if (node != null) {
				node.accept(this);
			}
		}

		return call;
	}

}
