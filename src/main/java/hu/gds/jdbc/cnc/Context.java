package hu.gds.jdbc.cnc;

import java.util.Map;
import java.util.function.Function;

/**
 * Context represents some state that is passed throughout the system.
 *
 * <p>There are various stages of context that can extend or embed each other. The
 * important part is that it can be inspected and exported into other formats.</p>
 */
public interface Context {

    /**
     * Export this context into the specified format.
     *
     * @param format the format to export into.
     * @return the exported format as a string representation.
     */
    String exportAsString(final ExportFormat format);

    /**
     * The format into which the context can be exported.
     */
    @FunctionalInterface
    interface ExportFormat extends Function<Map<String, Object>, String> {

        /**
         * Java "toString" basically.
         */
        ExportFormat STRING = Object::toString;
    }
}
