package hu.gds.jdbc.error;

import java.sql.SQLException;

public class ColumnIndexException extends SQLException {

    public ColumnIndexException(String param) {
        super(param);
    }
}
