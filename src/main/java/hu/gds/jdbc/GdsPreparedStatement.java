package hu.gds.jdbc;

import hu.arheu.gds.message.data.ConsistencyType;
import hu.gds.jdbc.error.InvalidParameterException;
import hu.gds.jdbc.error.TypeMismatchException;
import hu.gds.jdbc.util.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringJoiner;

import static java.lang.Math.max;

public class GdsPreparedStatement extends GdsBaseStatement implements PreparedStatement {
    //INSERT INTO MyGuests (firstname, lastname, email) VALUES (?, 'Smith', ?)

    //a felvágott SQL string, kiszedve belőle az összes ? -es rész.
    //{
    // "INSERT INTO MyGuests (firstname, lastname, email) VALUES (",
    // ", 'Smith', ",
    // ")"}
    private final String[] templateStrings;
    //A user által megadott értékek később.
    private final String[] inStrings;
    //a megadott értékekhez az SQL típusok.
    private final Integer[] types;
    // Some performance caches
    private final StringBuffer stringBuffer = new StringBuffer();

    private final static String TRUE = "true";
    private final static String FALSE = "false";
    private final static String NULL = "null";

    GdsPreparedStatement(@NotNull GdsJdbcConnection connection, @NotNull String sql) throws SQLException {
        super(connection);
        sql = sql.trim();
        List<String> v = new ArrayList<>();
        int lastParamEnd = 0;

        // The following two boolean switches are used to make sure we're not
        // counting "?" in either strings or metadata strings. For instance the
        // following query:
        //    select '?' "A ? value" from dual
        // doesn't have any parameters.

        boolean inString = false;
        boolean inMetaString = false;

        for (int i = 0; i < sql.length(); ++i) {
            if (sql.charAt(i) == '\'')
                inString = !inString;
            if (sql.charAt(i) == '"')
                inMetaString = !inMetaString;
            if ((sql.charAt(i) == '?') && (!(inString || inMetaString))) {
                v.add(sql.substring(lastParamEnd, i));
                lastParamEnd = i + 1;
            }
        }
        v.add(sql.substring(lastParamEnd));

        int size = v.size();
        inStrings = new String[size - 1];
        types = new Integer[size - 1];
        clearParameters();

        templateStrings = v.toArray(new String[0]);
    }

    /**
     * Helper - this compiles the SQL query from the various parameters This is
     * identical to toString() except it throws an exception if a parameter is
     * unused.
     *
     * @return the compiled query
     * @throws SQLException if an error occurs
     */
    protected synchronized String compileQuery() throws SQLException {
        stringBuffer.setLength(0);
        int i;

        for (i = 0; i < inStrings.length; ++i) {
            if (inStrings[i] == null)
                throw new InvalidParameterException("Parameter " + (i + 1) + " is incorrect (null)!");
            stringBuffer.append(templateStrings[i]).append(inStrings[i]);
        }
        stringBuffer.append(templateStrings[inStrings.length]);
        return stringBuffer.toString();
    }

    /**
     * In general, parameter values remain in force for repeated used of a
     * <code>Statement</code>. Setting a parameter value automatically clears
     * its previous value. However, in coms cases, it is useful to immediately
     * release the resources used by the current parameter values; this can be
     * done by calling <code>clearParameters()</code>.
     *
     * @throws SQLException if a database access error occurs
     */
    public void clearParameters() throws SQLException {
        checkClosed(false);
        int i;

        for (i = 0; i < inStrings.length; i++) {
            inStrings[i] = null;
            types[i] = null;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        if (currentResultSet == null) {
            throw new SQLException("No result set");
        }
        return currentResultSet;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    /**
     * This stores an Object into a parameter.
     *
     * @param parameterIndex the first parameter is 1...
     * @param value          the object to set
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void setObject(int parameterIndex, Object value) throws SQLException {
        if (value == null) {
            setNull(parameterIndex, Types.JAVA_OBJECT);
        } else {
            if (value instanceof String)
                setString(parameterIndex, (String) value);
            else if (value instanceof Short)
                setShort(parameterIndex, (Short) value);
            else if (value instanceof Integer)
                setInt(parameterIndex, (Integer) value);
            else if (value instanceof Long)
                setLong(parameterIndex, (Long) value);
            else if (value instanceof BigDecimal bigDecimal) {
                if (bigDecimal.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0 &&
                        bigDecimal.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
                    setLong(parameterIndex, bigDecimal.longValue());
                } else {
                    throw new TypeMismatchException("BigDecimal number is out of range for long values." +
                            "(" + value + ")");
                }
            } else if (value instanceof Float)
                setFloat(parameterIndex, (Float) value);
            else if (value instanceof Double)
                setDouble(parameterIndex, (Double) value);
            else if (value instanceof byte[])
                setBytes(parameterIndex, (byte[]) value);
            else if (value instanceof Boolean)
                setBoolean(parameterIndex, (Boolean) value);
            else if (value instanceof Array)
                setArray(parameterIndex, (Array) value);
            else if (value instanceof java.sql.Date)
                setDate(parameterIndex, (java.sql.Date) value);
            else if (value instanceof Time)
                setTime(parameterIndex, (Time) value);
            else if (value instanceof Timestamp)
                setTimestamp(parameterIndex, (Timestamp) value);
            else if (value instanceof Blob)
                setBlob(parameterIndex, (Blob) value);
            else if (value instanceof java.net.URL)
                setURL(parameterIndex, (java.net.URL) value);
            else
                throw new TypeMismatchException("Objects of type " + value.getClass()
                        + " are not supported.");
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate() throws SQLException {
        execute();
        return max(0, getUpdateCount());
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            return executeInner(compileQuery(), false, ConsistencyType.PAGES);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method should not be called on prepared statement");
    }

    /**
     * There are a lot of setXXX classes which all basically do the same thing. We
     * need a method which actually does the set for us.
     *
     * @param paramIndex the index into the inString
     * @param s          a string to be stored
     * @throws SQLException if something goes wrong
     */
    private void set(int paramIndex, String s, int sqlType) throws SQLException {
        checkClosed(false);
        if (paramIndex < 1 || paramIndex > inStrings.length)
            throw new InvalidParameterException("Parameter index out of range.");
        inStrings[paramIndex - 1] = s;
        types[paramIndex - 1] = sqlType;
    }

    /**
     * Sets a parameter to SQL NULL.
     * <p>
     * <b>Note: </b> you must specify the parameters SQL type but we ignore it.
     *
     * @param parameterIndex the first parameter is 1, etc...
     * @param sqlType        the SQL type code defined in java.sql.Types
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        set(parameterIndex, "null", sqlType);
    }


    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        set(parameterIndex, x ? TRUE : FALSE, Types.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        set(parameterIndex, Integer.toString(x), Types.TINYINT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        set(parameterIndex, Integer.toString(x), Types.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        set(parameterIndex, Integer.toString(x), Types.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        set(parameterIndex, Long.toString(x), Types.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        set(parameterIndex, Float.toString(x), Types.REAL);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        set(parameterIndex, Double.toString(x), Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.NUMERIC);
        } else {
            set(parameterIndex, x.toString(), Types.NUMERIC);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.VARCHAR);
        } else {
            set(parameterIndex, "'" + StringEscapeUtils.escape(x) + "'", Types.VARCHAR);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.BINARY);
        } else {
            set(parameterIndex, "0x" + byteArrayToHexString(x), Types.VARBINARY);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.DATE);
        } else {
            set(parameterIndex, "'" + new Date(x.getTime()) + "'", Types.DATE);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.TIME);
        } else {
            set(parameterIndex, "{t '" + x + "'}", Types.TIME);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            // Be careful don't use instanceof here since it would match derived
            // classes.
            if (x.getClass().equals(Timestamp.class))
                set(parameterIndex, "'" + x + "'", Types.TIMESTAMP);
            else
                set(parameterIndex, "'" + new Timestamp(x.getTime()) + "'", Types.TIMESTAMP);
        }
    }

    /**
     * When a very large ASCII value is input to a LONGVARCHAR parameter, it may
     * be more practical to send it via a java.io.InputStream. JDBC will read the
     * data from the stream as needed, until it reaches end-of-file. The JDBC
     * driver will do any necessary conversion from ASCII to the database char
     * format.
     * <p>
     * <b>Note: </b> this stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x              the parameter value
     * @param length         the number of bytes in the stream
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
    }

    /**
     * When a very large Unicode value is input to a LONGVARCHAR parameter, it may
     * be more practical to send it via a java.io.InputStream. JDBC will read the
     * data from the stream as needed, until it reaches end-of-file. The JDBC
     * driver will do any necessary conversion from UNICODE to the database char
     * format.
     * <p>** DEPRECIATED IN JDBC 2 **
     * <p>
     * <b>Note: </b> this stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x              the parameter value
     * @param length         the parameter length
     * @throws SQLException if a database access error occurs
     * @deprecated
     */
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.BINARY);
        } else {
            byte[] data = new byte[length];
            try {
                int offset = 0;
                int read;
                do {
                    read = x.read(data, offset, length - offset);
                    offset += read;
                } while (length > offset && -1 != read);
            } catch (Exception ioe) {
                throw new SQLException("Problem with streaming of data", ioe);
            }
            setBytes(parameterIndex, data);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void addBatch() throws SQLException {
        super.addBatch(compileQuery());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (null == reader) {
            setNull(parameterIndex, Types.VARCHAR);
        } else {
            char[] data = new char[length];
            try {
                int read;
                int offset = 0;
                do {
                    read = reader.read(data, offset, length - offset);
                    offset += read;
                } while (length > offset && -1 != read);
            } catch (Exception ioe) {
                throw new SQLException("Problem with streaming of data");
            }
            setString(parameterIndex, new String(data));
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.BLOB);
        } else if (x.length() > Integer.MAX_VALUE) {
            throw new SQLException(String.format("The length of the Blob object is too big and would underflow as an int! (value: %1$s)", x.length()));
        } else {
            setBinaryStream(parameterIndex, x.getBinaryStream(), (int) x.length());
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.CLOB);
        } else if (x.length() > Integer.MAX_VALUE) {
            throw new SQLException(String.format("The length of the Blob object is too big and would underflow as an int! (value: %1$s)", x.length()));
        } else {
            setString(parameterIndex, x.getSubString(0, (int) x.length()));
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.ARRAY);
        } else {
            Object[] array = (Object[]) x.getArray();
            StringJoiner joiner = new StringJoiner(", ", "ARRAY(", ")");
            for (Object value : array) {
                if (value == null) {
                    joiner.add(NULL);
                } else {
                    if (value instanceof String)
                        joiner.add("'").add(StringEscapeUtils.escape((String) value)).add("'");
                    else if (value instanceof Number)
                        joiner.add(value.toString());
                    else if (value instanceof Boolean)
                        joiner.add((Boolean) value ? TRUE : FALSE);
                    else if (value instanceof Date)
                        joiner.add("'").add(new Date(((Date) value).getTime()).toString()).add("'");
                    else if (value instanceof Time)
                        joiner.add("{t '").add(value.toString()).add("'}");
                    else if (value instanceof Timestamp)
                        // Be careful don't use instanceof here since it would match derived
                        // classes.
                        if (value.getClass().equals(Timestamp.class))
                            joiner.add("'").add(value.toString()).add("'");
                        else
                            joiner.add("'").add(new Timestamp(((Timestamp) value).getTime()).toString()).add("'");
                    else
                        throw new TypeMismatchException("Objects of type " + value.getClass()
                                + " are not supported.");
                }
            }
            set(parameterIndex, joiner.toString(), Types.ARRAY);
        }
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet rs = null;
        try {
            rs = getResultSet();
        } catch (Throwable ignored) {
        }
        if (rs != null)
            return rs.getMetaData();
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.DATE);
        } else {
            if (cal == null)
                setDate(parameterIndex, x);
            else {
                cal.setTime(x);
                setDate(parameterIndex, new Date(cal.getTime().getTime()));
            }
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIME);
        } else {
            if (cal == null)
                setTime(parameterIndex, x);
            else {
                cal.setTime(x);
                setTime(parameterIndex, new Time(cal.getTime().getTime()));
            }
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            if (cal == null)
                setTimestamp(parameterIndex, x);
            else {
                cal.setTime(x);
                setTimestamp(parameterIndex, new java.sql.Timestamp(cal.getTime().getTime()));
            }
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
//        return new GdsParameterMetaData(connection, table, inStrings, types);
        return new GdsParameterMetaData(inStrings, types);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
        } else {
            switch (targetSqlType) {
                case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> setInt(parameterIndex, ((Number) x).intValue());
                case Types.BIGINT -> setLong(parameterIndex, ((Number) x).longValue());
                case Types.REAL, Types.FLOAT, Types.DOUBLE, Types.DECIMAL, Types.NUMERIC ->
                    // Cast to Number is not necessary
                        set(parameterIndex, x.toString(), targetSqlType);
                case Types.BIT, Types.BOOLEAN -> setBoolean(parameterIndex, (Boolean) x);
                case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> setString(parameterIndex, (String) x);
                case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> setBytes(parameterIndex, (byte[]) x);
                case Types.DATE -> setDate(parameterIndex, (Date) x);
                case Types.TIME -> setTime(parameterIndex, (Time) x);
                case Types.TIMESTAMP -> setTimestamp(parameterIndex, (Timestamp) x);
                case Types.BLOB -> setBlob(parameterIndex, (Blob) x);
                case Types.DATALINK -> setURL(parameterIndex, (URL) x);
                case Types.JAVA_OBJECT, Types.OTHER -> setObject(parameterIndex, x);
                default -> throw new TypeMismatchException("Unsupported type value: " + targetSqlType);
            }
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (length > Integer.MAX_VALUE) {
            throw new SQLException(String.format("The length of the stream object is too big and would underflow as an int! (value: %1$s)", length));
        }
        setBinaryStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (null == x) {
            setNull(parameterIndex, Types.BINARY);
        } else {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int nRead;
                byte[] data = new byte[1024];
                while ((nRead = x.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }

                buffer.flush();
                setBytes(parameterIndex, buffer.toByteArray());
            } catch (Exception ioe) {
                throw new SQLException("Problem with streaming of data", ioe);
            }
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
