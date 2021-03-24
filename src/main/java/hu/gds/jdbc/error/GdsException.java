package hu.gds.jdbc.error;

import java.sql.SQLException;

public class GdsException extends SQLException {

    public GdsException(String param) {
        super(param);
    }

    public GdsException(String param, Throwable throwable) {
        super(param, throwable);
    }

    public GdsException(Throwable throwable) {
        super(throwable);
    }
}
