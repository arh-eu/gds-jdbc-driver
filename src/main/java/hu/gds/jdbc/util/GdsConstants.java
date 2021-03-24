package hu.gds.jdbc.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GdsConstants {
    private GdsConstants() {
    }

    public static final String ANALYZER = "analyzer";

    public static final String ATTACHMENT_TABLE_SUFFIX = "-@attachment\"";

    public final static String ID_FIELD = "id";
    public final static String DATA_FIELD = "data";
    public final static String OWNER_ID_FIELD = "ownerid";
    public final static String META_FIELD = "meta";

    public static final String TIMESTAMP_FIELD = "@timestamp";
    public static final String TO_VALID_FIELD = "@to_valid";
    public static final String TTL_FIELD = "@ttl";
    public static final String VERSION_FIELD = "@@version";


    public static boolean isReadOnlyField(String field) {
        return ONLY_READABLE_FIELDS.contains(field);
    }

    public static final List<String> ONLY_READABLE_FIELDS = Collections.unmodifiableList(Arrays.asList(
            TIMESTAMP_FIELD,
            TO_VALID_FIELD,
            VERSION_FIELD
    ));
}
