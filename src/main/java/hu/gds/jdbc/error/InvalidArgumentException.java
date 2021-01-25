package hu.gds.jdbc.error;

public class InvalidArgumentException extends GdsException {

    public InvalidArgumentException(String message, Throwable cause, ErrorContext ctx) {
        super(message, cause, ctx);
    }

    public static InvalidArgumentException fromMessage(final String message) {
        return new InvalidArgumentException(message, null, null);
    }

    public static InvalidArgumentException fromMessage(final String message, final Throwable cause) {
        return new InvalidArgumentException(message, cause, null);
    }

}
