package hu.gds.jdbc.executor;

import hu.arheu.gds.message.data.ConsistencyType;
import hu.gds.jdbc.GdsBaseStatement;
import hu.gds.jdbc.GdsJdbcConnection;
import hu.gds.jdbc.resultset.DQLResultSet;

import java.util.List;

public class DQLExecutor {
    private final DQLResultSet result;

    public DQLExecutor(boolean attachmentDQL,
                       GdsJdbcConnection connection,
                       String sql,
                       String tableName,
                       List<String> attachmentSelectedFields,
                       GdsBaseStatement statement,
                       ConsistencyType consistencyType) throws Throwable {
        result = new DQLResultSet(attachmentDQL, sql, tableName, attachmentSelectedFields, statement, consistencyType);
    }

    public DQLResultSet getResult() {
        return result;
    }
}
