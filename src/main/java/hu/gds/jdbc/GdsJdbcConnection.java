package hu.gds.jdbc;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class GdsJdbcConnection implements Connection {
    private static final String DEFAULT_SCHEMA = "default";
    private final GdsConnection gdsConnection;
    private final GdsJdbcDriver gdsJdbcDriver;
    private boolean isClosed = false;
    private boolean isReadOnly = false;
    private final Properties info;
    private final GdsClientURI gdsClientURI;

    public GdsJdbcConnection(GdsClientURI gdsClientURI, GdsConnection gdsConnection, GdsJdbcDriver gdsJdbcDriver, Properties info) {
        this.gdsClientURI = gdsClientURI;
        this.gdsConnection = gdsConnection;
        this.gdsJdbcDriver = gdsJdbcDriver;
        this.info = info;
    }

    public Statement createStatement() throws SQLException {
        checkClosed();
        try {
            return new GdsStatement(this);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        try {
            return new GdsPreparedStatement(this, sql);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("GDS does not support SQL natively.");
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    public void commit() throws SQLException {
        checkClosed();
    }

    public void rollback() throws SQLException {
        checkClosed();
    }

    public void close() throws SQLException {
        if (!isClosed) {
            gdsConnection.close();
        }
        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new GdsDatabaseMetaData(this, gdsJdbcDriver);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        isReadOnly = readOnly;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setCatalog(String catalog) {
    }

    public String getCatalog() throws SQLException {
        checkClosed();
        return null;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        // Since the only valid value for MongDB is Connection.TRANSACTION_NONE, and the javadoc for this method
        // indicates that this is not a valid value for level here, throw unsupported operation exception.
        if (level != Connection.TRANSACTION_READ_COMMITTED) {
            throw new UnsupportedOperationException("GDS only supports dirty read (READ_COMMITTED) during transactions!");
        }
    }

    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException();
        }
        return prepareStatement(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob createNClob() throws SQLException {
        checkClosed();
        return null;
    }

    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return null;
    }

    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
        return true;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return info.getProperty(name);
    }

    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return info;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        return null;
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return null;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    public void setSchema(String schema) {
        setCatalog(schema);
    }

    public String getSchema() {
        return DEFAULT_SCHEMA;
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkClosed();
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();
        return false;
    }

    public GdsConnection getGdsConnection() {
        return gdsConnection;
    }

    public GdsClientURI getGdsClientURI() {
        return gdsClientURI;
    }
}
