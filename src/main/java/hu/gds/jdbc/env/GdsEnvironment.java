package hu.gds.jdbc.env;

import hu.gds.jdbc.encryption.CryptoManager;
import hu.gds.jdbc.error.InvalidArgumentException;

import java.util.Optional;

import static hu.gds.jdbc.util.Validators.notNull;

public class GdsEnvironment {
    private final Optional<CryptoManager> cryptoManager;

    private GdsEnvironment(GdsEnvironment.Builder builder) {
        this.cryptoManager = Optional.ofNullable(builder.cryptoManager);
    }

    protected String defaultAgentTitle() {
        return "java";
    }

    public static GdsEnvironment create() {
        return builder().build();
    }

    public static GdsEnvironment.Builder builder() {
        return new GdsEnvironment.Builder();
    }

    public Optional<CryptoManager> cryptoManager() {
        return this.cryptoManager;
    }

    public static class Builder {
        private CryptoManager cryptoManager;
        private SecurityConfig.Builder securityConfig;

        Builder() {
        }

        public GdsEnvironment.Builder cryptoManager(CryptoManager cryptoManager) {
            this.cryptoManager = cryptoManager;
            return this;
        }

        public GdsEnvironment build() {
            return new GdsEnvironment(this);
        }

        /**
         * Allows to configure everything related to TLS/encrypted connections.
         * <p>
         *
         * @param securityConfig the custom security config to use.
         * @return this {@link GdsEnvironment.Builder} for chaining purposes.
         */
        public Builder securityConfig(final SecurityConfig.Builder securityConfig) throws InvalidArgumentException {
            this.securityConfig = notNull(securityConfig, "SecurityConfig");
            return this;
        }

        /**
         * Returns the currently stored config builder.
         *
         * @return the current builder.
         */
        public SecurityConfig.Builder securityConfig() {
            return securityConfig;
        }
    }
}
