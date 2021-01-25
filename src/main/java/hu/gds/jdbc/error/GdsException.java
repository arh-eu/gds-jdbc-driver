package hu.gds.jdbc.error;

import hu.gds.jdbc.annotation.Stability;
import hu.gds.jdbc.cnc.Context;

public class GdsException extends RuntimeException {

    private final ErrorContext ctx;

    /**
     * Keeping it in there to not break left and right, but must be removed eventually to force good errors.
     */
    @Deprecated
    public GdsException() {
        this.ctx = null;
    }

    /**
     * Keeping it in there to not break left and right, but must be removed eventually to force good errors.
     */
    @Deprecated
    public GdsException(Throwable cause) {
        super(cause);
        this.ctx = null;
    }

    public GdsException(final String message) {
        this(message, (ErrorContext) null);
    }

    public GdsException(final String message, final ErrorContext ctx) {
        super(message);
        this.ctx = ctx;
    }

    public GdsException(final String message, final Throwable cause) {
        this(message, cause, null);
    }

    public GdsException(final String message, final Throwable cause, final ErrorContext ctx) {
        super(message, cause);
        this.ctx = ctx;
    }

    @Override
    public String toString() {
        final String output = super.toString();
        return ctx != null ? output + " " + ctx.exportAsString(Context.ExportFormat.STRING) : output;
    }

    /**
     * Returns the error context, if present.
     */
    @Stability.Uncommitted
    public ErrorContext context() {
        return ctx;
    }

}
