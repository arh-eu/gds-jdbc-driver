package hu.gds.jdbc.resultset;

import hu.gds.jdbc.GdsBaseStatement;
import hu.gds.jdbc.GdsJdbcConnection;
import hu.gds.jdbc.GdsStatement;
import hu.gds.jdbc.error.*;
import hu.gds.jdbc.types.JavaTypes;
import hu.gds.jdbc.util.GdsConstants;
import hu.gds.jdbc.util.ObjectToValueConverter;
import org.msgpack.value.NumberValue;
import org.msgpack.value.Value;

import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static hu.gds.jdbc.resultset.MsgPackValueHelper.*;

public abstract class AbstractGdsResultSet implements ResultSet {
    protected int index = 0;
    protected List<Value> currentRow = null;

    protected List<List<Value>> rows;

    protected boolean isClosed = false;
    protected boolean wasNull = false;
    protected final Map<String, Integer> fieldsIndexMap = new LinkedHashMap<>();

    final static long QUERY_TIMEOUT_DEFAULT = 10_000L;
    final static int RETRY_ON_ERROR_DEFAULT = 3;

    long timeout;
    final boolean attachmentDQL;
    final GdsBaseStatement statement;
    GdsResultSetMetaData metaData;

    protected String queryId;
    protected int rowNumber = 0;

    protected final String sql;

    private Closeable activeStream;

    private boolean onInsertRow = false;
    private boolean updateInProcess = false;

    private HashMap<String, Object> updateValues = null;

    protected final GdsJdbcConnection gdsJdbcConnection;

    public AbstractGdsResultSet(boolean attachmentDQL, String sql, GdsBaseStatement statement) throws SQLException {
        this.attachmentDQL = attachmentDQL;
        this.sql = sql;
        this.statement = statement;
        this.gdsJdbcConnection = (GdsJdbcConnection) statement.getConnection();
    }

    public AbstractGdsResultSet(boolean attachmentDQL, String sql, GdsJdbcConnection gdsJdbcConnection) {
        this.attachmentDQL = attachmentDQL;
        this.sql = sql;
        this.statement = null;
        this.gdsJdbcConnection = gdsJdbcConnection;
    }

    protected List<Value> getCurrentRow() {
        return this.currentRow;
    }

    private void replaceRowValue(int columnIndex, Object value) throws SQLException {
        checkClosedAndCurrentRow();
        checkColumnIndex(columnIndex);
        try {
            currentRow.set(columnIndex - 1, ObjectToValueConverter.convert(value));
        } catch (Throwable throwable) {
            throw new SQLException(throwable);
        }
    }

    private void replaceRowValue(String columnLabel, Object value) throws SQLException {
        replaceRowValue(findColumn(columnLabel), value);
    }

    public abstract boolean isDml();

    public abstract boolean isDql();

    public abstract DQLResultSet asDqlResultSet() throws SQLException;

    public abstract DMLResultSet asDmlResultSet() throws SQLException;

    protected abstract String getTableName();

    private void closeActiveStream() throws SQLException {
        if (activeStream != null) {
            try {
                activeStream.close();
            } catch (IOException e) {
                throw new SQLException(e);
            } finally {
                activeStream = null;
            }
        }
    }

    private void setActiveStream(Closeable closeable) {
        activeStream = closeable;
    }

    long longValueFromString(String value, String identifier, long defaultValue) {
        if (value == null || "".equals(value.trim())) {
            return defaultValue;
        } else {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("The value of the '" + identifier + "' parameter is invalid!", nfe);
            }
        }
    }

    void setupTimeout(String value) {
        this.timeout = longValueFromString(value, "timeout", QUERY_TIMEOUT_DEFAULT);
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new ClosedResultSetException(sql);
        }
    }

    private void checkCurrentRow() throws SQLException {
        if (currentRow == null) {
            throw new ExhaustedResultSetException(sql);
        }
    }

    void checkClosedAndCurrentRow() throws SQLException {
        checkClosed();
        checkCurrentRow();
    }

    private void checkColumnIndex(int i) throws SQLException {
        if (currentRow != null) {
            if (i < 1 || i > currentRow.size()) {
                throw new SQLException("The column index is out of range: " + i + ", number of columns: " + currentRow.size() + ").", sql, -1);
            }
        }
    }

    private boolean isNull(Value value) {
        return value == null || value.isNilValue();
    }

    private Value getColumnValue(int columnIndex) throws SQLException {
        checkClosedAndCurrentRow();
        checkColumnIndex(columnIndex);
        closeActiveStream();
        wasNull = false;
        Value result = currentRow.get(columnIndex - 1);
        if (isNull(result)) {
            wasNull = true;
        }
        return result;
    }

    private String getColumnLabel(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        for (Map.Entry<String, Integer> entry : fieldsIndexMap.entrySet()) {
            if (entry.getValue() == columnIndex) {
                return entry.getKey();
            }
        }
        throw new ColumnIndexException("The column index is out of range: " + columnIndex + ", number of columns: " + currentRow.size() + ").");
    }

    private String getMimeType(int columnIndex) throws SQLException {
        return metaData.getMimeType(columnIndex);
    }

    private NumberValue getNumeric(int columnIndex, JavaTypes targetType) throws SQLException {
        return getNumberValueFromValue(getColumnValue(columnIndex), targetType);
    }

    private void checkUpdatable() throws SQLException {
        checkClosed();
        if (updateValues == null) {
            updateValues = new HashMap<>();
        }
    }

    private void updateValue(int columnIndex, Object value) throws SQLException {
        checkUpdatable();
        checkColumnIndex(columnIndex);
        updateInProcess = !onInsertRow;
        //if (value == null) {
        //    updateNull(columnIndex);
        //} else {
        checkColumnIndex(columnIndex);
        String columnName = metaData.getColumnName(columnIndex);
        updateValues.put(columnName, value);
        //}
    }

    private void checkAttachmentTypeColumnOperation(int columnIndex) throws SQLException {
        if (!attachmentDQL) {
            throw new SQLException("Get bytes only allow by attachment response! The request is a standard query and not an attachment query.", sql, -1);
        }
        String columnLabel = getColumnLabel(columnIndex);
        if (!columnLabel.equals(GdsConstants.DATA_FIELD)) {
            throw new SQLException("Only on data field allowed to get the bytes, but found " + columnLabel + " field", sql, -1);
        }
    }

    @Override
    public void close() {
        this.currentRow = null;
        this.rows = null;
        isClosed = true;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>String</code> value or the <code>String</code> value returned by the <code>toString()</code> for other types;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the result set is closed or if the column index is out of range
     */
    @Override
    public String getString(int columnIndex) throws SQLException {
        return getStringFromValue(getColumnValue(columnIndex), false);
    }

    /**
     * @param columnIndex the index of the column
     * @return if the value is a numeric fixed point number, the returned value will be <code>true</code>
     * if it's value is <code>1</code> and false if it's value is <code>0</code>;
     * if the value is a string, the returned value will be <code>true</code> if it's value is <code>"1"</code>
     * and false if it's value is <code>"0"</code>;
     * if the value is <code>null</code>, the returned value will be false
     * @throws SQLException in combination with all other types and values;
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBooleanFromValue(getColumnValue(columnIndex));
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>byte</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column index is out of range
     */
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getNumeric(columnIndex, JavaTypes.BYTE).toByte();
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>short</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column index is out of range
     */
    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getNumeric(columnIndex, JavaTypes.BYTE).toShort();
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>int</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column index is out of range
     */
    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getNumeric(columnIndex, JavaTypes.INTEGER).toInt();
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>long</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column index is out of range
     */
    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getNumeric(columnIndex, JavaTypes.LONG).toLong();
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>float</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column index is out of range
     */
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getNumeric(columnIndex, JavaTypes.FLOAT).toFloat();
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>double</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column index is out of range
     */
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getNumeric(columnIndex, JavaTypes.DOUBLE).toDouble();
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    /**
     * @param columnIndex the index of the column
     * @return the raw value if this query is an attachment query and if the field is an attachment field
     * (the name of the column is 'data'); <code>null</code> if the value is <code>null</code>
     * @throws SQLException if it is not an attachment query or if the field is not an attachment field;
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Value value = getColumnValue(columnIndex);
        if (isNull(value)) {
            return null;
        }
        checkAttachmentTypeColumnOperation(columnIndex);
        return value.asBinaryValue().asByteArray();
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>Date</code> object from the timestamp value if the value is a numeric fixed point number
     * and if the mime type of the field is 'datetime'; UTC time-zone will be used;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not a fixed point number value or if the mime type of the value is not 'datetime';
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        if (timestamp == null) {
            return null;
        }
        return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>Time</code> object from the timestamp value if the value is a numeric fixed point number
     * and if the mime type of the field is 'datetime'; UTC time-zone will be used;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not a fixed point number value or if the mime type of the value is not 'datetime';
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        if (timestamp == null) {
            return null;
        }
        return Time.valueOf(timestamp.toLocalDateTime().toLocalTime());
    }

    /**
     * @param columnIndex the index of the column
     * @return the <code>Timestamp</code> object from the timestamp value if the value is a numeric fixed point number
     * and if the mime type of the field is 'datetime'; UTC time-zone will be used;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not a fixed point number value or if the mime type of the value is not 'datetime';
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestampFromValue(getColumnValue(columnIndex), getMimeType(columnIndex));
    }

    /**
     * @param columnIndex the index of the column
     * @return the ASCII encoded <code>InputStream</code> if the value is <code>String</code;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if any error occurs during the encoding;
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (value == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII));
        setActiveStream(stream);
        return stream;
    }

    /**
     * @param columnIndex the index of the column
     * @return the UTF-8 encoded <code>InputStream</code> if the value is <code>String</code;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if any error occurs during the encoding;
     *                      if the result set is closed or if the column index is out of range
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (value == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
        setActiveStream(stream);
        return stream;
    }

    /**
     * @param columnIndex the index of the column
     * @return the binary <code>InputStream</code> if this query is an attachment query and if the field is an attachment field
     * the name of the column is 'data')
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if this query is not an attachment query or if the field is not an attachment field;
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        byte[] value = getBytes(columnIndex);
        if (value == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(value);
        setActiveStream(stream);
        return stream;
    }

    /**
     * @param columnIndex the index of the column
     * @return the value as <code>Array</code> type; possible array element types are
     * <code>String</code>, <code>Boolean</code> and the numeric value types;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not an array type or if a not allowed array item type found;
     *                      if the result set is closed or if the column index is out of range
     */
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getArrayFromValue(getColumnValue(columnIndex), metaData.getSubType(columnIndex));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>String</code> value or the <code>String</code> value returned by the <code>toString()</code> for other types;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the result set is closed or if the column name is not found
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return if the value is a numeric fixed point number, the returned value will be <code>true</code>
     * if it's value is <code>1</code> and false if it's value is <code>0</code>;
     * if the value is a string, the returned value will be <code>true</code> if it's value is <code>"1"</code>
     * and false if it's value is <code>"0"</code>;
     * if the value is <code>null</code>, the returned value will be false
     * @throws SQLException in combination with all other types and values;
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>byte</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column name is not found
     */
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>short</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column name is not found
     */
    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>int</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column name is not found
     */
    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>long</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value has a floating point type, a conversion according to Java will occur;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column name is not found
     */
    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>float</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column name is not found
     */
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>double</code> representation of the value if it fits in the byte range corresponding to Java;
     * if the value is <code>null</code>, the value returned is <code>0</code>
     * @throws SQLException if the value is not fits in the byte range corresponding to Java;
     *                      if the value is not a numeric value; if the result set is closed or if the column name is not found
     */
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    /**
     * @param columnLabel the name of the column
     * @return the raw value if this query is an attachment query and if the field is an attachment field
     * (the name of the column is 'data'); <code>null</code> if the value is <code>null</code>
     * @throws SQLException if it is not an attachment query or if the field is not an attachment field;
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>Date</code> object from the timestamp value if the value is a numeric fixed point number
     * and if the mime type of the field is 'datetime'; UTC time-zone will be used;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not a fixed point number value or if the mime type of the value is not 'datetime';
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>Time</code> object from the timestamp value if the value is a numeric fixed point number
     * and if the mime type of the field is 'datetime'; UTC time-zone will be used;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not a fixed point number value or if the mime type of the value is not 'datetime';
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the <code>Timestamp</code> object from the timestamp value if the value is a numeric fixed point number
     * and if the mime type of the field is 'datetime'; UTC time-zone will be used;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if the value is not a fixed point number value or if the mime type of the value is not 'datetime';
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the ASCII encoded <code>InputStream</code> if the value is <code>String</code;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if any error occurs during the encoding;
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the UTF-8 encoded <code>InputStream</code> if the value is <code>String</code;
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if any error occurs during the encoding;
     *                      if the result set is closed or if the column name is not found
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    /**
     * @param columnLabel the name of the column
     * @return the binary <code>InputStream</code> if this query is an attachment query and if the field is an attachment field
     * the name of the column is 'data')
     * <code>null</code> if the value is <code>null</code>
     * @throws SQLException if this query is not an attachment query or if the field is not an attachment field;
     *                      if the result set is closed or if the column name is not found
     */
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObjectFromValue(getColumnValue(columnIndex), metaData.getType(columnIndex), metaData.getSubType(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer index = fieldsIndexMap.get(columnLabel);
        if (index != null) {
            return index;
        } else {
            index = fieldsIndexMap.get(columnLabel.toLowerCase());
            if (index != null) {
                return index;
            } else {
                index = fieldsIndexMap.get(columnLabel.toUpperCase());
                if (index != null) {
                    return index;
                }
            }
        }
        throw new InvalidParameterException("The column name " + columnLabel + " was not found in this ResultSet.");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (value == null) {
            return null;
        }
        return new CharArrayReader(value.toCharArray());
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        this.checkClosed();
        if (onInsertRow) {
            return false;
        }
        if (this.isDql() && this.asDqlResultSet().attachmentDQL) {
            return index == 0;
        } else {
            if (rows == null || rows.isEmpty()) {
                return false;
            } else {
                return index <= 0;
            }
        }
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        this.checkClosed();
        if (onInsertRow) {
            return false;
        }
        if (this.isDql() && this.asDqlResultSet().attachmentDQL) {
            return index == 1;
        } else {
            if (rows == null || rows.isEmpty()) {
                return false;
            } else {
                return index == 1;
            }
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        this.checkClosed();
        if (onInsertRow) {
            return false;
        }
        if (this.isDql() && this.asDqlResultSet().attachmentDQL) {
            return index == 1;
        } else {
            if (rows == null || rows.isEmpty()) {
                return false;
            } else {
                return index == rows.size();
            }
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        updateValue(columnIndex, null);
    }

    @Override
    public synchronized void updateBoolean(int columnIndex, boolean x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateByte(int columnIndex, byte x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateShort(int columnIndex, short x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateInt(int columnIndex, int x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateLong(int columnIndex, long x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateFloat(int columnIndex, float x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateDouble(int columnIndex, double x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public synchronized void updateString(int columnIndex, String x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateBytes(int columnIndex, byte[] x) throws SQLException {
        checkAttachmentTypeColumnOperation(columnIndex);
        StringBuilder hexStringBuffer = new StringBuilder();
        for (byte b : x) {
            char[] hexDigits = new char[2];
            hexDigits[0] = Character.forDigit((b >> 4) & 0xF, 16);
            hexDigits[1] = Character.forDigit((b & 0xF), 16);
            hexStringBuffer.append(hexDigits);
        }
        this.updateValue(columnIndex, "0x" + hexStringBuffer);
    }

    private void updateTimestamp(int columnIndex, Long value) throws SQLException {
        if (value == null) {
            updateNull(columnIndex);
        }
        this.updateValue(columnIndex, value);
    }

    @Override
    public synchronized void updateDate(int columnIndex, Date x) throws SQLException {
        updateTimestamp(columnIndex, x.getTime());
    }

    @Override
    public synchronized void updateTime(int columnIndex, Time x) throws SQLException {
        updateTimestamp(columnIndex, x.getTime());
    }

    @Override
    public synchronized void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        updateTimestamp(columnIndex, x.getTime());
    }

    @Override
    public synchronized void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        if (x == null) {
            updateNull(columnIndex);
        } else {
            try {
                updateCharacterStream(columnIndex, new InputStreamReader(x, StandardCharsets.US_ASCII), length);
            } catch (Throwable throwable) {
                throw new SQLException(throwable);
            }
        }
    }

    @Override
    public synchronized void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        if (x == null) {
            updateNull(columnIndex);
        } else {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] data = new byte[length];
                int nRead;
                while ((nRead = x.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
                updateBytes(columnIndex, buffer.toByteArray());
            } catch (Throwable throwable) {
                throw new SQLException(throwable);
            }
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        if (x == null) {
            updateNull(columnIndex);
        } else {
            try {
                char[] data = new char[length];
                int numRead = 0;
                do {
                    int n = x.read(data, numRead, length - numRead);
                    if (n == -1) {
                        break;
                    }
                    numRead += n;
                } while (numRead != length);
                updateString(columnIndex, new String(data, 0, numRead));
            } catch (Throwable throwable) {
                throw new SQLException(throwable);
            }
        }
    }

    @Override
    public synchronized void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public synchronized void updateObject(int columnIndex, Object x) throws SQLException {
        this.updateValue(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    private void checkUpdateValuesMap(boolean checkId) throws SQLException {
        if (updateValues == null || updateValues.isEmpty()) {
            throw new SQLException("There is no value that can be inserted/updated.");
        }
        if (checkId) {
            if (!updateValues.containsKey(GdsConstants.ID_FIELD)) {
                throw new InvalidParameterException("The field " + GdsConstants.ID_FIELD + " cannot found. The " + GdsConstants.ID_FIELD + " field is required.");
            } else if (updateValues.get(GdsConstants.ID_FIELD) == null) {
                throw new InvalidParameterException("The field " + GdsConstants.ID_FIELD + " cannot be null.");
            }
        }
    }

    private DQLResultSet getDqlResultSet(String id) throws SQLException {
        List<String> columnNamesList = metaData.getColumnNames();
        StringJoiner columnNames = new StringJoiner(", ");
        for (String columnName : columnNamesList) {
            columnNames.add("\"" + columnName + "\"");
        }
        String selectSql = "SELECT " + columnNames + " FROM " + "\"" + getTableName() + "\"" + " WHERE " + GdsConstants.ID_FIELD + "='" + id + "'";
        GdsStatement selectStatement = (GdsStatement) statement.getConnection().createStatement();
        selectStatement.executeInnerQuery(selectSql);
        return (DQLResultSet) selectStatement.getResultSet();
    }

    private void addToDqlResultSet(String id) throws SQLException {
        DQLResultSet resultSet = getDqlResultSet(id);
        if (resultSet.next()) {
            this.asDqlResultSet().addRow(resultSet.getCurrentRow());
        }
    }

    private void addToDmlResultSet(DMLResultSet resultSet) throws SQLException {
        DMLResultSet thisDmlResultSet = this.asDmlResultSet();
        while (resultSet.next()) {
            thisDmlResultSet.addRow(resultSet.getCurrentRow(), resultSet.getTableName(), resultSet.rowInserted(), resultSet.rowUpdated());
        }
    }

    private void addToResultSet(Statement statement) throws SQLException {
        if (isDml()) {
            DMLResultSet resultSet = (DMLResultSet) statement.getResultSet();
            addToDmlResultSet(resultSet);
        } else if (isDql()) {
            addToDqlResultSet((String) updateValues.get(GdsConstants.ID_FIELD));
        } else {
            throw new SQLException("The ResultSet is not a dml and not a dql ResultSet.");
        }
    }

    private String arrayToString(Object[] values) {
        if (values == null) {
            return "null";
        }
        StringJoiner arrayStringJoiner = new StringJoiner(",", "array(", ")");
        for (Object o : values) {
            if (o == null) {
                arrayStringJoiner.add("null");
            } else if (o instanceof String) {
                arrayStringJoiner.add("'" + o + "'");
            } else {
                arrayStringJoiner.add(o.toString());
            }
        }
        return arrayStringJoiner.toString();
    }

    @Override
    public void insertRow() throws SQLException {
        checkUpdatable();
        if (null != getTableName() && getTableName().endsWith(GdsConstants.ATTACHMENT_TABLE_SUFFIX)) {
            throw new SQLException("Insert to an attachment table is not allowed.", sql, -1);
        }
        if (!onInsertRow) {
            throw new SQLException("Not on the insert row.");
        }
        checkUpdateValuesMap(true);
        String insertSql = "INSERT INTO " + getTableName() + " ";
        StringJoiner insertColumns = new StringJoiner(",", "(", ") ");
        StringJoiner insertValues = new StringJoiner(",", "VALUES(", ")");
        for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
            insertColumns.add(entry.getKey());
            if (entry.getValue() == null) {
                insertValues.add("null");
            } else if (entry.getValue() instanceof Object[]) {
                insertValues.add(arrayToString((Object[]) entry.getValue()));
            } else if (entry.getValue() instanceof String) {
                insertValues.add("'" + entry.getValue() + "'");
            } else {
                insertValues.add(entry.getValue().toString());
            }
        }
        insertSql += insertColumns.toString();
        insertSql += insertValues.toString();
        Statement insertStatement = statement.getConnection().createStatement();
        insertStatement.execute(insertSql);
        addToResultSet(insertStatement);
    }

    @Override
    public void updateRow() throws SQLException {
        checkUpdatable();
        if (onInsertRow) {
            throw new SQLException("Cannot call updateRow() when on the insert row.");
        }
        if (!this.isBeforeFirst()) {
            if (updateInProcess) {
                checkUpdateValuesMap(false);
                String updateSql = "UPDATE " + getTableName() + " SET ";
                StringJoiner updateSetClause = new StringJoiner(", ");
                for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    if (!key.equals(GdsConstants.ID_FIELD)) {
                        String temp = key + "=";
                        if (value == null) {
                            temp += "null";
                        } else if (value instanceof String) {
                            temp += "'" + value + "'";
                        } else if (value instanceof Object[]) {
                            temp += arrayToString((Object[]) value);
                        } else {
                            temp += value.toString();
                        }
                        updateSetClause.add(temp);
                    }
                }
                updateSql += updateSetClause.toString();
                updateSql += " WHERE " + GdsConstants.ID_FIELD + "=" + "'" + this.getString(GdsConstants.ID_FIELD) + "'";
                Statement updateStatement = statement.getConnection().createStatement();
                updateStatement.execute(updateSql);
                for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
                    replaceRowValue(entry.getKey(), entry.getValue());
                }
                updateValues.clear();
                updateInProcess = false;
            }
        } else {
            throw new SQLException("Cannot update the ResultSet because it is before the start.");
        }
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void refreshRow() throws SQLException {
        if (onInsertRow) {
            throw new SQLException("Cannot call updateRow() when on the insert row.");
        }
        if (!this.isBeforeFirst()) {
            String id = getString(GdsConstants.ID_FIELD);
            if (id != null) {
                DQLResultSet resultSet = getDqlResultSet(id);
                if (resultSet.next()) {
                    this.currentRow = resultSet.currentRow;
                }
            }
        } else {
            throw new SQLException("Cannot update the ResultSet because it is before the start.");
        }
    }

    @Override
    public synchronized void cancelRowUpdates() throws SQLException {
        checkClosed();
        if (onInsertRow) {
            throw new SQLException("Cannot call cancelRowUpdates() when on the insert row.");
        }
        if (updateInProcess) {
            updateInProcess = false;
        }
        if (updateValues != null) {
            updateValues.clear();
        }
    }

    @Override
    public synchronized void moveToInsertRow() {
        onInsertRow = true;
        updateInProcess = false;
    }

    @Override
    public synchronized void moveToCurrentRow() {
        onInsertRow = false;
        updateInProcess = false;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String value = getStringFromValue(getColumnValue(columnIndex), true);
        if (value == null) {
            return null;
        }
        try {
            return new URL(value);
        } catch (MalformedURLException exception) {
            throw new SQLException(exception);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        updateValue(columnIndex, x.getArray());
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public int getHoldability() {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateNString(findColumn(columnLabel), nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        try {
            return type.cast(getObject(columnIndex));
        } catch (Exception ex) {
            throw new TypeMismatchException(ex);
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        try {
            return type.cast(getObject(columnLabel));
        } catch (Exception ex) {
            throw new TypeMismatchException(ex);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }
}