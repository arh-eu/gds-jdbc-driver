package hu.gds.jdbc.error;

import java.sql.SQLException;

public class ClosedResultSetException extends SQLException {
    public ClosedResultSetException(String reason) {
        super(reason);
    }

    public ClosedResultSetException() {
        super();
    }

    public ClosedResultSetException(Throwable cause) {
        super(cause);
    }
}
