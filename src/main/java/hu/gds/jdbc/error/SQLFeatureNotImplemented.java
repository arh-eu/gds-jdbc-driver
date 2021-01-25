package hu.gds.jdbc.error;

import java.sql.SQLException;

public class SQLFeatureNotImplemented extends SQLException {

    public SQLFeatureNotImplemented() {
        super("Not implemented yet.");
    }
}
