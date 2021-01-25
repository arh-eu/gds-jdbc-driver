package hu.gds.jdbc.resultset;

import hu.arheu.gds.message.data.FieldValueType;
import hu.gds.jdbc.GdsJdbcConnection;
import hu.gds.jdbc.types.JavaTypes;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GdsResultSetMetaData implements ResultSetMetaData {

    private static final String NOT_APPLICABLE_CATALOG = "";
    private static final int NOT_APPLICABLE_SCALE = 0;
    private static final int NOT_APPLICABLE_PRECISION = 0;

    private final List<ColumnMetaData> columnMetaData;
    private final String tableName;
    private final GdsJdbcConnection connection;

    public GdsResultSetMetaData(@NotNull List<ColumnMetaData> columnMetaData, String tableName, GdsJdbcConnection connection) {
        this.columnMetaData = columnMetaData;
        this.tableName = tableName;
        this.connection = connection;
    }

    public static GdsResultSetMetaData.ColumnMetaData createColumn(String name, JavaTypes javaType, JavaTypes javaSubType, String mimeType, FieldValueType gdsType) {
        return new GdsResultSetMetaData.ColumnMetaData(name, javaType, javaSubType, mimeType, gdsType);
    }

    public List<String> getColumnNames() {
        List<String> columnNames = new ArrayList<>();
        for(ColumnMetaData c: columnMetaData) {
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
        ColumnMetaData c = columnMetaData.get(column - 1);
        Statement stmt;
        String newTableName=tableName;
        if(tableName.startsWith("\"")) {
            newTableName = tableName.replaceAll("\"", "");
        }
        String sql = "SELECT * FROM \"@gds.config.store.schema\" WHERE table = '" + newTableName + "'";
        ResultSet rs;
        switch(c.javaType.getTypeName()) {
            case "string":
                stmt = connection.createStatement();
                try {
                    rs = stmt.executeQuery(sql);
                    while(rs.next()) {
                        if(rs.getString("field_name").equals(c.name)) {
                            String an;
                            an = rs.getString("analyzer");
                            if (an == null) {
                                an = "keyword";
                            }
                            switch (an) {
                                case "keyword":
                                case "whitespace":
                                    return true;
                                default:
                                    return false;
                            }
                        }
                    }
                    return true;
                } catch(SQLException e) {
                    return true;
                }
            case "array":
            case "map":
                if ("string".equals(c.javaSubType.getTypeName())) {
                    stmt = connection.createStatement();
                    try {
                        rs = stmt.executeQuery(sql);
                        while(rs.next()) {
                            if(rs.getString("field_name").equals(c.name)) {
                                String an;
                                an = rs.getString("analyzer");
                                if (an == null) {
                                    an = "keyword";
                                }
                                switch (an) {
                                    case "keyword":
                                    case "whitespace":
                                        return true;
                                    default:
                                        return false;
                                }
                            }
                        }
                        return true;
                    } catch(SQLException e) {
                        return true;
                    }
                }
            default:
                return false;
        }
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        final String ATTACHMENT_TABLE_SUFFIX = "-@attachment\"";
        if(tableName.endsWith(ATTACHMENT_TABLE_SUFFIX)) {
            String s = columnMetaData.get(column-1).name;
            return "id".equals(s) || "ownerid".equals(s);
        }
        Statement stmt = connection.createStatement();
        String sql = "SELECT * FROM \"@gds.config.store.schema\" WHERE table = '" + tableName + "'";
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()) {
            if(rs.getString("field_name").equals(columnMetaData.get(column-1).name)) {
                return rs.getBoolean("searchable");
            }
        }
        throw new SQLException("Cannot find column: " + column + ". (Aliased columns are not supported with isSearchable(..)!");
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
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkColumnIndex(column);
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
        return columnMetaData.get(column -1).getMimeType();
    }

    private void checkColumnIndex(int column) throws SQLException {
        if(column<1 || column>columnMetaData.size()) {
            throw new SQLException("The column index is out of range: " + column + ", number of columns: " + columnMetaData.size() + ").");
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
            int displaySize = 0;
            switch (gdsType) {
                case KEYWORD:
                case KEYWORD_ARRAY:
                    displaySize = 32768;
                    break;
                case TEXT:
                case BINARY:
                case BINARY_ARRAY:
                case TEXT_ARRAY:
                case STRING_MAP:
                    displaySize = Integer.MAX_VALUE;
                    break;
                case BOOLEAN:
                case BOOLEAN_ARRAY:
                    displaySize = 5; //FALSE
                    break;
                case DOUBLE:
                case DOUBLE_ARRAY:
                    /*
                     * The maximum display size of a double.
                     * Example: -3.3333333333333334E-100
                     */
                    displaySize = 24;
                    break;
                case INTEGER:
                case INTEGER_ARRAY:
                    /*
                     * The maximum display size of an int.
                     * Example: -2147483648
                     */
                    displaySize = 11;
                    break;
                case LONG:
                case LONG_ARRAY:
                    /*
                     * The maximum display size of a long.
                     * Example: 9223372036854775808
                     */
                    displaySize = 20;
                    break;
            }
            return displaySize;
        }

        public static int getScale(FieldValueType gdsType) {
            if(gdsType.equals(FieldValueType.DOUBLE) || gdsType.equals(FieldValueType.DOUBLE_ARRAY)) {
                return 16;
            }
            else {
                return NOT_APPLICABLE_SCALE;
            }
        }

        public static int getPrecision(FieldValueType gdsType) {
            int precision;
            switch (gdsType) {
                case KEYWORD:
                case KEYWORD_ARRAY:
                    precision = 32768;
                    break;
                case TEXT:
                case BINARY:
                case BINARY_ARRAY:
                case TEXT_ARRAY:
                case STRING_MAP:
                    precision = Integer.MAX_VALUE;
                    break;
                case BOOLEAN:
                case BOOLEAN_ARRAY:
                    precision = 5; //FALSE
                    break;
                case DOUBLE:
                case DOUBLE_ARRAY:
                    precision = 17;
                    break;
                case INTEGER:
                case INTEGER_ARRAY:
                    precision = 10;
                    break;
                case LONG:
                case LONG_ARRAY:
                    precision = 19;
                    break;
                default:
                    precision = NOT_APPLICABLE_PRECISION;
            }
            return precision;
        }

        public static boolean isSigned(FieldValueType gdsType) {
            switch(gdsType) {
                case DOUBLE:
                case DOUBLE_ARRAY:
                case INTEGER:
                case INTEGER_ARRAY:
                case LONG:
                case LONG_ARRAY:
                    return true;
                default:
                    return false;
            }
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
