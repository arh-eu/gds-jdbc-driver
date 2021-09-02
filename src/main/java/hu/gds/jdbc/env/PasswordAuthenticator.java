package hu.gds.jdbc.env;

import hu.gds.jdbc.error.InvalidArgumentException;

import java.util.function.Supplier;

import static hu.gds.jdbc.util.Validators.notNull;
import static hu.gds.jdbc.util.Validators.notNullOrEmpty;

/**
 * Performs authentication against a couchbase server cluster using username and password.
 */
public class PasswordAuthenticator implements Authenticator {

    private final Supplier<String> username;
    private final Supplier<String> password;

    /**
     * Creates a new {@link PasswordAuthenticator.Builder} which allows to customize this authenticator.
     */
    public static PasswordAuthenticator.Builder builder() {
        return new PasswordAuthenticator.Builder();
    }

    /**
     * Creates a new password authenticator with the default settings.
     *
     * @param username the username to use for all authentication.
     * @param password the password to use alognside the username.
     * @return the instantiated {@link PasswordAuthenticator}.
     */
    public static PasswordAuthenticator create(final String username, final String password) throws InvalidArgumentException {
        return builder().username(username).password(password).build();
    }

    private PasswordAuthenticator(final PasswordAuthenticator.Builder builder) throws InvalidArgumentException {
        this.username = notNull(builder.username, "username");
        this.password = notNull(builder.password, "password");
    }

    /**
     * Provides customization to the {@link PasswordAuthenticator}.
     */
    public static class Builder {

        private Supplier<String> username;
        private Supplier<String> password;

        /**
         * Specifies a static username that will be used for all authentication purposes.
         *
         * @param username the username to use.
         * @return this builder for chaining purposes.
         */
        public PasswordAuthenticator.Builder username(final String username) throws InvalidArgumentException {
            notNullOrEmpty(username, "Username");
            return username(new Supplier<String>() {
                @Override
                public String get() {
                    return username;
                }
            });
        }

        /**
         * Specifies a dynamic username that will be used for all authentication purposes.
         * <p>
         * Every time the SDK needs to authenticate against the server, it will re-evaluate the supplier. This means that
         * you can pass in a supplier that dynamically loads a username from a (remote) source without taking the application
         * down on a restart.
         * <p>
         * It is VERY IMPORTANT that this supplier must not block on IO. It is called in async contexts and blocking for
         * a longer amount of time will stall SDK resources like async event loops.
         *
         * @param username the username to use.
         * @return this builder for chaining purposes.
         */
        public PasswordAuthenticator.Builder username(final Supplier<String> username) throws InvalidArgumentException {
            notNull(username, "Username");
            this.username = username;
            return this;
        }

        /**
         * Specifies a static password that will be used for all authentication purposes.
         *
         * @param password the password to alongside for the username provided.
         * @return this builder for chaining purposes.
         */
        public PasswordAuthenticator.Builder password(final String password) throws InvalidArgumentException {
            notNullOrEmpty(password, "Password");
            return password(new Supplier<String>() {
                @Override
                public String get() {
                    return password;
                }
            });
        }

        /**
         * Specifies a dynamic password that will be used for all authentication purposes.
         * <p>
         * Every time the SDK needs to authenticate against the server, it will re-evaluate the supplier. This means that
         * you can pass in a supplier that dynamically loads a password from a (remote) source without taking the application
         * down on a restart.
         * <p>
         * It is VERY IMPORTANT that this supplier must not block on IO. It is called in async contexts and blocking for
         * a longer amount of time will stall SDK resources like async event loops.
         *
         * @param password the password to alongside for the username provided.
         * @return this builder for chaining purposes.
         */
        public PasswordAuthenticator.Builder password(final Supplier<String> password) throws InvalidArgumentException {
            notNull(password, "Password");
            this.password = password;
            return this;
        }

        /**
         * Creates the {@link PasswordAuthenticator} based on the customization in this builder.
         *
         * @return the created password authenticator instance.
         */
        public PasswordAuthenticator build() throws InvalidArgumentException {
            return new PasswordAuthenticator(this);
        }
    }

}
