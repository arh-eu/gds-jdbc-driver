package hu.gds.jdbc.executor;

import hu.gds.jdbc.GdsBaseStatement;
import hu.gds.jdbc.GdsJdbcConnection;
import hu.gds.jdbc.resultset.DMLResultSet;

import java.util.Map;

public class DMLExecutor {
    private final DMLResultSet result;

    public DMLExecutor(Map<String, byte[]> attachments,
                       boolean onlyAttachmentDML,
                       GdsJdbcConnection connection,
                       String sql,
                       GdsBaseStatement statement) throws Throwable {
        result = new DMLResultSet(attachments, onlyAttachmentDML, sql, statement);
    }

    public DMLResultSet getResult() {
        return result;
    }
}
