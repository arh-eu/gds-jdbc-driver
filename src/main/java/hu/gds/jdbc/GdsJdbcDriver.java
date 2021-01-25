package hu.gds.jdbc;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import static hu.gds.jdbc.GdsClientURI.PREFIX;
/**
 * Minimal implementation of the JDBC standards for the GDS database.
 */
public class GdsJdbcDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new GdsJdbcDriver());
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Connect to the database using a URL like :
     * jdbc:gds:host1[:port1]...][?option=value[&option=value&...]]
     * The URL's hosts and ports configuration is passed as it is to the GDS native Java driver.
     */
    @Override
    public Connection connect(@NotNull String url, @Nullable Properties info) throws SQLException {
        if (acceptsURL(url)) {
            try {
                GdsClientURI clientURI = new GdsClientURI(url, info);
                GdsConnection gdsConnection = clientURI.createGdsConnection();
                if (info == null) {
                    info = new Properties();
                }
                return new GdsJdbcConnection(clientURI, gdsConnection, this, info);
            } catch (Exception e) {
                throw new SQLException(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * URLs accepted are of the form:
     * jdbc:gds:host1[:port1]...][?option=value[&option=value&...]]
     */
    @Override
    public boolean acceptsURL(@NotNull String url) {
        return url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DriverPropertyInfoHelper.getPropertyInfo();
    }

    String getVersion() {
        return "1.0.0";
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
