package hu.gds.jdbc.resultset;

import hu.arheu.gds.message.data.FieldValueType;
import hu.gds.jdbc.GdsJdbcConnection;
import hu.gds.jdbc.error.ColumnIndexException;
import hu.gds.jdbc.error.InvalidParameterException;
import hu.gds.jdbc.types.JavaTypes;
import hu.gds.jdbc.util.GdsConstants;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;

public class GdsResultSetMetaData implements ResultSetMetaData {

    private static final String NOT_APPLICABLE_CATALOG = "";
    private static final int NOT_APPLICABLE_SCALE = 0;
    private static final int NOT_APPLICABLE_PRECISION = 0;

    private final List<ColumnMetaData> columnMetaData;
    private final String tableName;
    private final GdsJdbcConnection connection;

    private final Set<String> caseSensitiveFields;
    private final Map<String, Boolean> searchableCache;
    private final Map<String, Integer> columnNameToIndex;

    private boolean cacheCalculated = false;

    public GdsResultSetMetaData(@NotNull List<ColumnMetaData> columnMetaData, String tableName, GdsJdbcConnection connection) {
        this.columnMetaData = columnMetaData;
        this.tableName = tableName;
        this.connection = connection;
        this.caseSensitiveFields = new HashSet<>();
        this.searchableCache = new HashMap<>();
        this.columnNameToIndex = new HashMap<>();

        for (int ii = 0; ii < columnMetaData.size(); ++ii) {
            columnNameToIndex.put(columnMetaData.get(ii).name, ii);
        }
    }

    private void calculateCachedData() throws SQLException {
        String newTableName = tableName;
        if (tableName.startsWith("\"")) {
            newTableName = tableName.replaceAll("\"", "");
        }
        String sql = "SELECT * FROM \"@gds.config.store.schema\" WHERE table = '" + newTableName + "'";

        ResultSet rs = connection.createStatement().executeQuery(sql);
        while (rs.next()) {
            String field = rs.getString("field_name");
            String analyzer = null;
            if (columnNameToIndex.containsKey(GdsConstants.ANALYZER)) {
                analyzer = rs.getString(GdsConstants.ANALYZER);
            }

            searchableCache.put(field, rs.getBoolean("searchable"));
            String javaTypeName = columnMetaData.get(columnNameToIndex.get(field)).javaType.getTypeName();

            if ("string".equals(javaTypeName) || "array".equals(javaTypeName) || "map".equals(javaTypeName)) {
                if ((null == analyzer) || "keyword".equals(analyzer) || "whitespace".equals(analyzer)) {
                    caseSensitiveFields.add(field);
                }
            }
        }

        searchableCache.putIfAbsent(GdsConstants.TO_VALID_FIELD, false);
        searchableCache.putIfAbsent(GdsConstants.TTL_FIELD, false);
    }

    public static GdsResultSetMetaData.ColumnMetaData createColumn(String name, JavaTypes javaType, JavaTypes javaSubType, String mimeType, FieldValueType gdsType) {
        return new GdsResultSetMetaData.ColumnMetaData(name, javaType, javaSubType, mimeType, gdsType);
    }

    public List<String> getColumnNames() {
        List<String> columnNames = new ArrayList<>();
        for (ColumnMetaData c : columnMetaData) {
            columnNames.add(c.name);
        }
        return columnNames;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getColumnCount() {
        return this.columnMetaData.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumnIndex(column);
        checkCache();
        String columnName = columnMetaData.get(column - 1).name;
        return caseSensitiveFields.contains(columnName);
    }

    private void checkCache() throws SQLException {
        if (!cacheCalculated) {
            calculateCachedData();
            cacheCalculated = true;
        }
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        checkCache();
        String columnName = columnMetaData.get(column - 1).name;

        if (tableName.endsWith(GdsConstants.ATTACHMENT_TABLE_SUFFIX)) {
            return GdsConstants.ID_FIELD.equals(columnName) || GdsConstants.OWNER_ID_FIELD.equals(columnName);
        }

        //@timestamp is always searchable.
        if (columnName.equals(GdsConstants.TIMESTAMP_FIELD)) {
            return true;
        }

        //@@version is never searchable.
        if (columnName.equals(GdsConstants.VERSION_FIELD)) {
            return false;
        }

        //it should be cached
        if (searchableCache.containsKey(columnName)) {
            return searchableCache.get(columnName);
        }

        throw new InvalidParameterException("Cannot find column: " + column + ". (Aliased columns are not supported with isSearchable(..)!");
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkColumnIndex(column);
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).isSigned;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).displaySize;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).name;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        checkColumnIndex(column);
        return getCatalogName(column);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).precision;
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).scale;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkColumnIndex(column);
        return tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkColumnIndex(column);
        return NOT_APPLICABLE_CATALOG;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).getJavaType().getSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).getJavaType().getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkColumnIndex(column);
        String columnName = columnMetaData.get(column - 1).name;
        return GdsConstants.isReadOnlyField(columnName);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkColumnIndex(column);
        String columnName = columnMetaData.get(column - 1).name;
        return !GdsConstants.isReadOnlyField(columnName);
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).getJavaType().getClassName();
    }

    public JavaTypes getType(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).getJavaType();
    }

    public JavaTypes getSubType(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).getJavaSubType();
    }

    public String getMimeType(int column) throws SQLException {
        checkColumnIndex(column);
        return columnMetaData.get(column - 1).getMimeType();
    }

    private void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columnMetaData.size()) {
            throw new ColumnIndexException("The column index is out of range: " + column + ", number of columns: " + columnMetaData.size() + ").");
        }
    }

    public static class ColumnMetaData {
        private final String name;
        private final JavaTypes javaType;
        private final JavaTypes javaSubType;
        private final String mimeType;
        private final int displaySize;
        private final int scale;
        private final int precision;
        private final boolean isSigned;

        private ColumnMetaData(String name, JavaTypes javaType, JavaTypes javaSubType, String mimeType, FieldValueType gdsType) {
            this.name = name;
            this.javaType = javaType;
            this.javaSubType = javaSubType;
            this.mimeType = mimeType;
            this.displaySize = getDisplaySize(gdsType);
            this.scale = getScale(gdsType);
            this.precision = getPrecision(gdsType);
            this.isSigned = isSigned(gdsType);
        }

        public static int getDisplaySize(FieldValueType gdsType) {
            return switch (gdsType) {
                case KEYWORD, KEYWORD_ARRAY -> 32768;
                case TEXT, BINARY, BINARY_ARRAY, TEXT_ARRAY, STRING_MAP, GEO_POINT, GEO_POINT_ARRAY, GEO_SHAPE, GEO_SHAPE_ARRAY, DATE_TIME, DATE_TIME_ARRAY ->
                        Integer.MAX_VALUE;
                case BOOLEAN, BOOLEAN_ARRAY -> 5; //FALSE
                case DOUBLE, DOUBLE_ARRAY ->
                    /*
                     * The maximum display size of a double.
                     * Example: -3.3333333333333334E-100
                     */
                        24;
                case INTEGER, INTEGER_ARRAY ->
                    /*
                     * The maximum display size of an int.
                     * Example: -2147483648
                     */
                        11;
                case LONG, LONG_ARRAY ->
                    /*
                     * The maximum display size of a long.
                     * Example: 9223372036854775808
                     */
                        20;
            };
        }

        public static int getScale(FieldValueType gdsType) {
            if (gdsType.equals(FieldValueType.DOUBLE) || gdsType.equals(FieldValueType.DOUBLE_ARRAY)) {
                return 16;
            } else {
                return NOT_APPLICABLE_SCALE;
            }
        }

        public static int getPrecision(FieldValueType gdsType) {
            return switch (gdsType) {
                case KEYWORD, KEYWORD_ARRAY -> 32768;
                case TEXT, BINARY, BINARY_ARRAY, TEXT_ARRAY, STRING_MAP, GEO_POINT, GEO_POINT_ARRAY, GEO_SHAPE, GEO_SHAPE_ARRAY, DATE_TIME, DATE_TIME_ARRAY ->
                        Integer.MAX_VALUE;
                case BOOLEAN, BOOLEAN_ARRAY -> 5; //FALSE
                case DOUBLE, DOUBLE_ARRAY -> 17;
                case INTEGER, INTEGER_ARRAY -> 10;
                case LONG, LONG_ARRAY -> 19;
                default -> NOT_APPLICABLE_PRECISION;
            };
        }

        public static boolean isSigned(FieldValueType gdsType) {
            return switch (gdsType) {
                case DOUBLE, DOUBLE_ARRAY, INTEGER, INTEGER_ARRAY, LONG, LONG_ARRAY -> true;
                default -> false;
            };
        }

        public JavaTypes getJavaType() {
            return this.javaType;
        }

        public JavaTypes getJavaSubType() {
            return this.javaSubType;
        }

        public String getMimeType() {
            return this.mimeType;
        }
    }
}
