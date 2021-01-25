package hu.gds.jdbc;

import hu.gds.jdbc.query.QueryScanConsistency;

import java.sql.DriverPropertyInfo;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DriverPropertyInfoHelper {
    public static final String ENABLE_SSL = "sslenabled";
    public static final String KEYSTORE_PATH = "keyStorePath";
    public static final String KEYSTORE_PASSWORD = "keyStorePassword";
    public static final String VERIFY_SERVER_CERTIFICATE = "verifyServerCertificate";
    public static final String SERVE_ON_THE_SAME_CONNECTION = "serveOnTheSameConnection";
    public static final String TIMEOUT = "timeout";


    public static final String BOOLEAN_CHOICE_FALSE = "false";
    public static final String BOOLEAN_CHOICE_TRUE = "true";
    private static final String[] BOOL_CHOICES = new String[]{BOOLEAN_CHOICE_FALSE, BOOLEAN_CHOICE_TRUE};

    public static final String USER = "user";
    public static final String PASSWORD = "password";

    public static final String META_SAMPLING_SIZE = "meta.sampling.size";
    public static final int META_SAMPLING_SIZE_DEFAULT = 1000;

    public static final String RETRY_LIMIT_ON_ERROR = "retryLimitOnError";
    private static final String[] RETRY_LIMIT_CHOICES = new String[]{"0", "1", "2", "3", "5", "10"};

    public static final String DQL_QUERY_TYPE = "queryType";
    public static final String[] DQL_QUERY_TYPE_CHOICES = {"PAGE", "SCROLL"}; //0, 1

    public static final String DQL_QUERY_PAGE_SIZE = "queryPageSize";

//    public static final String DQL_CONSISTENCY_TYPE = "consistencyType";
//    public static final String[] DQL_CONSISTENCY_TYPE_CHOICES = {"NONE", "PAGE", "PAGES"};

    public static final String PREFETCH = "prefetch";


    public static DriverPropertyInfo[] getPropertyInfo() {
        ArrayList<DriverPropertyInfo> propInfos = new ArrayList<>();

        addPropInfo(propInfos, ENABLE_SSL, BOOLEAN_CHOICE_FALSE, "Enable SSL.", BOOL_CHOICES);
        addPropInfo(propInfos, VERIFY_SERVER_CERTIFICATE, BOOLEAN_CHOICE_TRUE,
                "Configure a connection that uses SSL but does not verify the identity of the server.",
                BOOL_CHOICES);
        addPropInfo(propInfos, SERVE_ON_THE_SAME_CONNECTION, BOOLEAN_CHOICE_TRUE,
                "Configure whether the replies from the GDS should be sent on this connection.",
                BOOL_CHOICES);
        addPropInfo(propInfos, USER, "", "Username used for login.", null);
        addPropInfo(propInfos, PASSWORD, "", "Password used for password authentication. " +
                "If left empty, no password will be used.", null);
        addPropInfo(propInfos, META_SAMPLING_SIZE, Integer.toString(META_SAMPLING_SIZE_DEFAULT),
                "Number of documents that will be fetched per collection in order " +
                        "to return meta information from DatabaseMetaData.getColumns() method.", null);
        addPropInfo(propInfos, DriverPropertyInfoHelper.ScanConsistency.QUERY_SCAN_CONSISTENCY,
                DriverPropertyInfoHelper.ScanConsistency.QUERY_SCAN_CONSISTENCY_DEFAULT.toString(),
                "Query scan consistency.",
                DriverPropertyInfoHelper.ScanConsistency.CHOICES);

        addPropInfo(propInfos, KEYSTORE_PATH, "", "Keystore path", null);
        addPropInfo(propInfos, KEYSTORE_PASSWORD, "", "Keystore password", null);
        addPropInfo(propInfos, TIMEOUT, "10000", "The timeout used for the statements in milliseconds. " +
                "By default (or if left empty) 10000ms will be used.", null);

        addPropInfo(propInfos, RETRY_LIMIT_ON_ERROR, "3", "Sets the limit for retries if any error " +
                "happens during the execution of the statement.", RETRY_LIMIT_CHOICES);

        addPropInfo(propInfos, DQL_QUERY_TYPE, "PAGE", "Sets whether to use types of scroll or page. Default value is page type.",
                DQL_QUERY_TYPE_CHOICES);
        addPropInfo(propInfos, DQL_QUERY_PAGE_SIZE, "-1", "Sets the page size of the queries. Default value is -1 to use the GDSs internal settings.",
                null);

//        addPropInfo(propInfos, DQL_CONSISTENCY_TYPE, "PAGE", "Sets the consistency type of the queries. By default, this will be 'PAGE'.",
//                DQL_CONSISTENCY_TYPE_CHOICES);


        addPropInfo(propInfos, PREFETCH, "3", "Sets the number of prefetches", null);

        return propInfos.toArray(new DriverPropertyInfo[0]);
    }

    private static void addPropInfo(final ArrayList<DriverPropertyInfo> propInfos, final String propName,
                                    final String defaultVal, final String description, final String[] choices) {
        DriverPropertyInfo newProp = new DriverPropertyInfo(propName, defaultVal);
        newProp.description = description;
        newProp.choices = choices;
        propInfos.add(newProp);
    }

    public static boolean isTrue(String value) {
        return value != null && (value.equals("1") || value.toLowerCase(Locale.ENGLISH).equals("true"));
    }

    public static class ScanConsistency {
        private ScanConsistency() {
            // empty
        }

        public static final String QUERY_SCAN_CONSISTENCY = "query.scan.consistency";
        public static final QueryScanConsistency QUERY_SCAN_CONSISTENCY_DEFAULT = QueryScanConsistency.NOT_BOUNDED;
        private static final Map<String, QueryScanConsistency> MAPPING = Collections.unmodifiableMap(Stream
                .of(new AbstractMap.SimpleEntry<>("not_bounded", QueryScanConsistency.NOT_BOUNDED),
                        new AbstractMap.SimpleEntry<>("request_plus", QueryScanConsistency.REQUEST_PLUS))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        private static final String[] CHOICES = MAPPING.keySet().toArray(new String[0]);

        public static QueryScanConsistency getQueryScanConsistency(Properties properties) {
            return getQueryScanConsistency(properties.getProperty(QUERY_SCAN_CONSISTENCY));
        }

        public static QueryScanConsistency getQueryScanConsistency(String scanConsistency) {
            if (scanConsistency == null) {
                return QUERY_SCAN_CONSISTENCY_DEFAULT;
            }
            return MAPPING.getOrDefault(scanConsistency, QUERY_SCAN_CONSISTENCY_DEFAULT);
        }
    }
}
