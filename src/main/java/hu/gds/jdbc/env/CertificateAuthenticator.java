package hu.gds.jdbc.env;

import io.netty.handler.ssl.SslContextBuilder;
import hu.gds.jdbc.error.InvalidArgumentException;

import javax.net.ssl.KeyManagerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static hu.gds.jdbc.util.Validators.notNull;
import static hu.gds.jdbc.util.Validators.notNullOrEmpty;

/**
 * Performs authentication through a client certificate instead of supplying username and password.
 */
public class CertificateAuthenticator implements Authenticator {

    private final PrivateKey key;
    private final String keyPassword;
    private final List<X509Certificate> keyCertChain;
    private final Supplier<KeyManagerFactory> keyManagerFactory;

    /**
     * Creates a new {@link CertificateAuthenticator} from a key store path.
     *
     * @param keyStorePath the file path to the keystore.
     * @param keyStorePassword the password for the keystore.
     * @param keyStoreType the type of the key store. If empty, the {@link KeyStore#getDefaultType()} will be used.
     * @return the created {@link CertificateAuthenticator}.
     */
    public static CertificateAuthenticator fromKeyStore(final Path keyStorePath, final String keyStorePassword,
                                                                                      final Optional<String> keyStoreType) {
        notNull(keyStorePath, "KeyStorePath");
        notNull(keyStoreType, "KeyStoreType");

        try {
            final KeyStore store = KeyStore.getInstance(keyStoreType.orElse(KeyStore.getDefaultType()));
            store.load(
                    Files.newInputStream(keyStorePath),
                    keyStorePassword != null ? keyStorePassword.toCharArray() : null
            );
            return fromKeyStore(store, keyStorePassword);
        } catch (Exception ex) {
            throw InvalidArgumentException.fromMessage("Could not initialize KeyStore from Path", ex);
        }
    }

    /**
     * Creates a new {@link CertificateAuthenticator} from a key store.
     *
     * @param keyStore the key store to load the certificate from.
     * @param keyStorePassword the password for the key store.
     * @return the created {@link CertificateAuthenticator}.
     */
    public static CertificateAuthenticator fromKeyStore(final KeyStore keyStore, final String keyStorePassword) {
        notNull(keyStore, "KeyStore");

        try {
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(
                    keyStore,
                    keyStorePassword != null ? keyStorePassword.toCharArray() : null
            );
            return fromKeyManagerFactory(() -> kmf);
        } catch (Exception ex) {
            throw InvalidArgumentException.fromMessage("Could not initialize KeyManagerFactory with KeyStore", ex);
        }
    }

    /**
     * Creates a new {@link CertificateAuthenticator} from a {@link KeyManagerFactory}.
     *
     * @param keyManagerFactory the key manager factory in a supplier that should be used.
     * @return the created {@link CertificateAuthenticator}.
     */
    public static CertificateAuthenticator fromKeyManagerFactory(final Supplier<KeyManagerFactory> keyManagerFactory) {
        notNull(keyManagerFactory, "KeyManagerFactory");
        return new CertificateAuthenticator(null, null, null, keyManagerFactory);
    }

    /**
     * Creates a new {@link CertificateAuthenticator} directly from a key and certificate chain.
     *
     * @param key the private key to authenticate.
     * @param keyPassword the password for to use.
     * @param keyCertChain the key certificate chain to use.
     * @return the created {@link CertificateAuthenticator}.
     */
    public static CertificateAuthenticator fromKey(final PrivateKey key, final String keyPassword,
                                                                                 final List<X509Certificate> keyCertChain) {
        notNull(key, "PrivateKey");
        notNullOrEmpty(keyCertChain, "KeyCertChain");
        return new CertificateAuthenticator(key, keyPassword, keyCertChain, null);
    }

    private CertificateAuthenticator(final PrivateKey key, final String keyPassword,
                                     final List<X509Certificate> keyCertChain,
                                     final Supplier<KeyManagerFactory> keyManagerFactory) {
        this.key = key;
        this.keyPassword = keyPassword;
        this.keyCertChain = keyCertChain;
        this.keyManagerFactory = keyManagerFactory;

        if (key != null && keyManagerFactory != null) {
            throw InvalidArgumentException.fromMessage("Either a key certificate or a key manager factory" +
                    " can be provided, but not both!");
        }
    }

    @Override
    public void applyTlsProperties(final SslContextBuilder context) {
        if (keyManagerFactory != null) {
            context.keyManager(keyManagerFactory.get());
        } else if (key != null) {
            context.keyManager(key, keyPassword, keyCertChain.toArray(new X509Certificate[0]));
        }
    }

    @Override
    public boolean supportsNonTls() {
        return false;
    }

    @Override
    public String toString() {
        return "CertificateAuthenticator{" +
                "key=" + key +
                ", keyCertChain=" + keyCertChain +
                ", keyManagerFactory=" + keyManagerFactory +
                '}';
    }

}
