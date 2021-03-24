package hu.gds.jdbc.error;

import java.sql.SQLException;

public class InvalidParameterException extends SQLException {

    public InvalidParameterException(String param) {
        super(param);
    }
}
