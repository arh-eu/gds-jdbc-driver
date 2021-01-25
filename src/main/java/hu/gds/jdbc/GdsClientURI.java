package hu.gds.jdbc;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.SQLException;
import java.util.*;

import static hu.gds.jdbc.DriverPropertyInfoHelper.*;

public class GdsClientURI {
    static final String PREFIX = "jdbc:gds:";

    private static final Set<String> JDBC_KEYS = new HashSet<>(Arrays.asList(
            USER, PASSWORD, SERVE_ON_THE_SAME_CONNECTION, ENABLE_SSL, KEYSTORE_PATH, KEYSTORE_PASSWORD, VERIFY_SERVER_CERTIFICATE));

    //private final String connectionString;
    final String uri;
    final String host;
    final String keyStorePath;
    final String keyStorePassword;
    final String userName;
    final String password;
    final String gateUrl;
    final boolean sslEnabled;
    final long timeout;
    final boolean serveOnTheSameConnection;
    final Integer queryType;
    final Integer queryPageSize;
//    final String consistencyType;
    final int prefetch;

    public GdsClientURI(@NotNull String uri, @Nullable Properties info) {
        this.uri = uri;
        if (!uri.startsWith(PREFIX)) {
            throw new IllegalArgumentException("URI needs to start with " + PREFIX);
        }

        String trimmedUri = uri.substring(PREFIX.length());
        Map<String, List<String>> options = null;
        String serverPart;
        String nsPart = "";

        int optionsStartIndex = trimmedUri.indexOf("?");
        if (optionsStartIndex >= 0) {
            serverPart = trimmedUri.substring(0, optionsStartIndex);
            options = parseOptions(trimmedUri.substring(optionsStartIndex + 1));
        } else {
            serverPart = trimmedUri;
        }

        int lastSlashIndex = serverPart.lastIndexOf("/");
        if (lastSlashIndex >= 0) {
            nsPart = serverPart.substring(lastSlashIndex + 1);
            serverPart = serverPart.substring(0, lastSlashIndex);
        }

        this.userName = getOption(info, options, USER, null);
        this.password = getOption(info, options, PASSWORD, null);
        this.keyStorePath = getOption(info, options, KEYSTORE_PATH, null);
        this.keyStorePassword = getOption(info, options, KEYSTORE_PASSWORD, null);
        this.sslEnabled = isTrue(getOption(info, options, ENABLE_SSL, BOOLEAN_CHOICE_FALSE));
        this.serveOnTheSameConnection = isTrue(getOption(info, options, SERVE_ON_THE_SAME_CONNECTION, BOOLEAN_CHOICE_TRUE));
        this.host = serverPart;
        this.gateUrl = nsPart;
        this.timeout = Long.parseLong(Objects.requireNonNull(getOption(info, options, TIMEOUT, Long.toString(10_000L))));
        String qt = Objects.requireNonNull(getOption(info, options, DQL_QUERY_TYPE, "PAGE")).toUpperCase();
        if (!"PAGE".equals(qt) && !"SCROLL".equals(qt)) {
            throw new IllegalArgumentException("QueryType must be one of " + Arrays.toString(DQL_QUERY_TYPE_CHOICES));
        }
        this.queryType = "PAGE".equals(qt) ? 0 : 1;
        this.queryPageSize = Integer.parseInt(Objects.requireNonNull(getOption(info, options, PREFETCH, "-1")));
//        this.consistencyType = getOption(info, options, DQL_CONSISTENCY_TYPE, "PAGE").toUpperCase();
//        if (!"PAGE".equals(this.consistencyType) && !"PAGES".equals(this.consistencyType) && !"NONE".equals(this.consistencyType)) {
//            throw new IllegalArgumentException("ConsistencyType must be one of " + Arrays.toString(DQL_CONSISTENCY_TYPE_CHOICES));
//        }
        this.prefetch = Integer.parseInt(Objects.requireNonNull(getOption(info, options, PREFETCH, "3")));
    }

    GdsConnection createGdsConnection() throws SQLException {
        GdsConnection gdsConnection;
        try {
            gdsConnection = new GdsConnection(this);
            gdsConnection.initConnection();
        } catch (Throwable ex) {
            throw new SQLException(ex);
        }
        return gdsConnection;
    }

    @Nullable
    private String getLastValue(@Nullable Map<String, List<String>> optionsMap, @NotNull String key) {
        if (optionsMap == null) return null;
        List<String> valueList = optionsMap.get(key);
        if (valueList == null || valueList.size() == 0) return null;
        return valueList.get(valueList.size() - 1);
    }

    /**
     * @return option from properties or from uri if it is not found in properties.
     * null if options was not found.
     */
    @Nullable
    private String getOption(@Nullable Properties properties, @Nullable Map<String, List<String>> options,
                             @NotNull String optionName, @Nullable String defaultValue) {
        if (properties != null) {
            String option = (String) properties.get(optionName);
            if (option != null) {
                return option;
            }
        }
        String value = getLastValue(options, optionName);
        return value != null ? value : defaultValue;
    }

    @NotNull
    private Map<String, List<String>> parseOptions(@NotNull String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<>();

        for (String _part : optionsPart.split("&")) {
            int idx = _part.indexOf("=");
            if (idx >= 0) {
                String key = _part.substring(0, idx).toLowerCase(Locale.ENGLISH);
                String value = _part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<>(1);
                }
                valueList.add(value);
                optionsMap.put(key, valueList);
            }
        }

        return optionsMap;
    }
}
