package hu.gds.jdbc.msg;

/**
 * The {@link ResponseStatus} describes what kind of response came back for a specific
 * request.
 *
 * <p>Note that this status is not tied to any protocol or service, rather there must
 * be a mapping performed from actual protocol-level response codes (be it http or
 * memcache protocol) to this abstract status. This allows to achieve a level of
 * consistency in status codes that is not tied to a particular protocol.</p>
 *
 * @since 1.0.0
 */
public enum ResponseStatus {
    /**
     * Indicates a successful response in general.
     */
    SUCCESS,
    /**
     * Indicates that the requested entity has not been found on the server.
     */
    NOT_FOUND,
    /**
     * The document exists (but maybe with another cas, depending on the op).
     */
    EXISTS,
    /**
     * Indicates an unknown status returned from the server, please check the
     * events/logs for further information.
     */
    UNKNOWN,
    /**
     * The server indicated that the given message failed because of a permission
     * violation.
     */
    NO_ACCESS,
    /**
     * The resource was not stored for some reason.
     */
    NOT_STORED,
    /**
     * The server could temporarily not fulfill the request.
     */
    TEMPORARY_FAILURE,
    /**
     * The server is busy for some reason.
     */
    SERVER_BUSY,
    /**
     * The server is out of memory.
     */
    OUT_OF_MEMORY,
    /**
     * The requested resource is locked.
     */
    LOCKED,
    /**
     * The server indicated that the given message is not supported.
     */
    UNSUPPORTED,
    /**
     * The server indicates that no bucket is selected.
     */
    NO_BUCKET,
    /**
     * In a kv request, signaling that the vbucket is on a different node.
     */
    NOT_MY_VBUCKET,
    /**
     * The written resource is too big.
     */
    TOO_BIG,
    /**
     * One or more attempted subdoc operations failed.
     */
    SUBDOC_FAILURE,
    /**
     * Invalid request. Returned if an invalid durability level is specified.
     */
    DURABILITY_INVALID_LEVEL,

    /**
     * Valid request, but given durability requirements are impossible to achieve.
     *
     * <p>because insufficient configured replicas are connected. Assuming level=majority and
     * C=number of configured nodes, durability becomes impossible if floor((C + 1) / 2)
     * nodes or greater are offline.</p>
     */
    DURABILITY_IMPOSSIBLE,

    /**
     * Returned if an attempt is made to mutate a key which already has a SyncWrite pending.
     *
     * <p>Transient, the client would typically retry (possibly with backoff). Similar to
     * ELOCKED.</p>
     */
    SYNC_WRITE_IN_PROGRESS,

    /**
     * Returned if the requested key has a SyncWrite which is being re-committed.
     *
     * <p>Transient, the client would typically retry (possibly with backoff). Similar to
     * ELOCKED.</p>
     */
    SYNC_WRITE_RE_COMMIT_IN_PROGRESS,

    /**
     * The SyncWrite request has not completed in the specified time and has ambiguous result.
     *
     * <p>it may Succeed or Fail; but the final value is not yet known.</p>
     */
    SYNC_WRITE_AMBIGUOUS,

    /**
     * The server indicated an internal error.
     */
    INTERNAL_SERVER_ERROR,

    TOO_MANY_REQUESTS,

    INVALID_ARGS,

    INVALID_REQUEST,

    UNKNOWN_COLLECTION,
    /**
     * The server reports that it is not initialized yet.
     */
    NOT_INITIALIZED;

    public boolean success() {
        return this == ResponseStatus.SUCCESS;
    }

}
