package hu.gds.jdbc.error;

import java.sql.SQLException;

public class TypeMismatchException extends SQLException {

    public TypeMismatchException(String param) {
        super(param);
    }

    public TypeMismatchException(Throwable throwable) {
        super(throwable);
    }
}
