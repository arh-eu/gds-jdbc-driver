package hu.gds.jdbc.encryption;

import hu.gds.jdbc.annotation.Stability;
import hu.gds.jdbc.util.CbStrings;

import java.util.Map;

/**
 * Provides low-level encryption routines for implementing Field-Level Encryption as specified by
 * <a href="https://github.com/couchbaselabs/sdk-rfcs/blob/master/rfc/0032-field-level-encryption.md">
 * Couchbase RFC-0032</a>.
 * <p>
 * An implementation knows how to encrypt and decrypt field values, and provides methods for
 * inspecting and transforming JSON field names to indicate whether a field holds an encrypted value.
 * <p>
 * {@code CryptoManager} is intended to be usable with any JSON library. The plaintext value
 * of a field is represented by a byte array containing valid JSON. The encrypted form is
 * represented by a Map which may be serialized as a JSON Object by your library of choice.
 * <p>
 * If you wish to encrypt or decrypt the fields of a Couchbase {@code JsonObject},
 * it may be more convenient to work with a higher level abstraction like the one provided
 * by the Java SDK's {@code JsonObjectCrypto} class.
 * <p>
 * Implementations must be thread-safe.
 */
@Stability.Volatile
public interface CryptoManager {
    /**
     * The name that refers to the default encrypter if one is present.
     */
    String DEFAULT_ENCRYPTER_ALIAS = "__DEFAULT__";

    /**
     * The prefix to use when mangling the names of encrypted fields
     * according to the default name mangling strategy.
     */
    String DEFAULT_ENCRYPTED_FIELD_NAME_PREFIX = "encrypted$";

    /**
     * Encrypts the given data using the named encrypter.
     *
     * @param plaintext the message to encrypt
     * @param encrypterAlias (nullable) alias of the encrypter to use, or null for default encrypter.
     * @return A map representing the encrypted form of the plaintext.
     */
    Map<String, Object> encrypt(byte[] plaintext, String encrypterAlias);

    /**
     * Selects an appropriate decrypter based on the contents of the
     * encrypted node and uses it to decrypt the data.
     *
     * @param encryptedNode the encrypted form of a message
     * @return the plaintext message
     */
    byte[] decrypt(Map<String, Object> encryptedNode);

    /**
     * Transforms the given field name to indicate its value is encrypted.
     */
    default String mangle(String fieldName) {
        return DEFAULT_ENCRYPTED_FIELD_NAME_PREFIX + fieldName;
    }

    /**
     * Reverses the transformation applied by {@link #mangle} and returns the original field name.
     */
    default String demangle(String fieldName) {
        return CbStrings.removeStart(fieldName, DEFAULT_ENCRYPTED_FIELD_NAME_PREFIX);
    }

    /**
     * Returns true if the given field name has been mangled by {@link #mangle(String)}.
     */
    default boolean isMangled(String fieldName) {
        return fieldName.startsWith(DEFAULT_ENCRYPTED_FIELD_NAME_PREFIX);
    }
}
