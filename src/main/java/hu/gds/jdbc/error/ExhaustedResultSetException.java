package hu.gds.jdbc.error;

import java.sql.SQLException;

public class ExhaustedResultSetException extends SQLException {
    public ExhaustedResultSetException(String reason) {
        super(reason);
    }

    public ExhaustedResultSetException() {
        super();
    }

    public ExhaustedResultSetException(Throwable cause) {
        super(cause);
    }
}
