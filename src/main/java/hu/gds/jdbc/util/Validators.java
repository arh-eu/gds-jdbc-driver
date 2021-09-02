package hu.gds.jdbc.util;

import hu.gds.jdbc.error.ErrorContext;
import hu.gds.jdbc.error.InvalidArgumentException;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Common validators used throughout the client.
 *
 * @since 2.0.0
 */
public class Validators {

    private Validators() {
    }

    public static <T> T notNullOrDefault(final T value, final T defaultValue) {
        return value != null ? value : defaultValue;
    }

    /**
     * Check if the given input is not null.
     *
     * <p>If it is null, a {@link InvalidArgumentException} is raised with a proper message.</p>
     *
     * @param input      the input to check.
     * @param identifier the identifier that is part of the exception message.
     */
    public static <T> T notNull(final T input, final String identifier) throws InvalidArgumentException {
        if (input == null) {
            throw InvalidArgumentException.fromMessage(identifier + " cannot be null");
        }
        return input;
    }

    public static <T> T notNull(final T input, final String identifier, final Supplier<ErrorContext> errorContext) throws InvalidArgumentException {
        try {
            return notNull(input, identifier);
        } catch (Exception cause) {
            throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
        }
    }

    /**
     * Check if the given string is not null or empty.
     *
     * <p>If it is null or empty, a {@link InvalidArgumentException} is raised with a
     * proper message.</p>
     *
     * @param input      the string to check.
     * @param identifier the identifier that is part of the exception message.
     */
    public static String notNullOrEmpty(final String input, final String identifier) throws InvalidArgumentException {
        if (input == null || input.isEmpty()) {
            throw InvalidArgumentException.fromMessage(identifier + " cannot be null or empty");
        }
        return input;
    }

    public static String notNullOrEmpty(final String input, final String identifier,
                                        final Supplier<ErrorContext> errorContext) throws InvalidArgumentException {
        try {
            return notNullOrEmpty(input, identifier);
        } catch (Exception cause) {
            throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
        }
    }

    public static <T> List<T> notNullOrEmpty(final List<T> input, final String identifier) throws InvalidArgumentException {
        if (input == null || input.isEmpty()) {
            throw InvalidArgumentException.fromMessage(identifier + " cannot be null or empty");
        }
        return input;
    }

    public static <T> List<T> notNullOrEmpty(final List<T> input, final String identifier,
                                             final Supplier<ErrorContext> errorContext) throws InvalidArgumentException {
        try {
            return notNullOrEmpty(input, identifier);
        } catch (Exception cause) {
            throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
        }
    }

    public static <T> Set<T> notNullOrEmpty(final Set<T> input, final String identifier) throws InvalidArgumentException {
        if (input == null || input.isEmpty()) {
            throw InvalidArgumentException.fromMessage(identifier + " cannot be null or empty");
        }
        return input;
    }

    public static <T> Set<T> notNullOrEmpty(final Set<T> input, final String identifier,
                                            final Supplier<ErrorContext> errorContext) throws InvalidArgumentException {
        try {
            return notNullOrEmpty(input, identifier);
        } catch (Exception cause) {
            throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
        }
    }

}
