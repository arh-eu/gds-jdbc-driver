package hu.gds.jdbc.env;

import hu.gds.jdbc.annotation.Stability;
import io.netty.handler.ssl.SslContextBuilder;

public interface Authenticator {

    @Stability.Internal
    default void applyTlsProperties(SslContextBuilder sslContextBuilder) {
    }

    @Stability.Internal
    default boolean supportsTls() {
        return true;
    }

    @Stability.Internal
    default boolean supportsNonTls() {
        return true;
    }
}
