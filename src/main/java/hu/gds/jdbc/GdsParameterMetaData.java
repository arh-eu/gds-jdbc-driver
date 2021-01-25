package hu.gds.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class GdsParameterMetaData implements ParameterMetaData {
    private final Object[] inStrings;
    private final Integer[] types;

    public GdsParameterMetaData(Object[] inStrings, Integer[] types) {
        this.inStrings = inStrings;
        this.types = types;
    }

    @Override
    public int getParameterCount() {
        return inStrings.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        if (param < 1 || param > inStrings.length)
            throw new SQLException("Parameter index out of range.");
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        if (param < 1 || param > inStrings.length)
            throw new SQLException("Parameter index out of range.");
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        if (param < 1 || param > inStrings.length)
            throw new SQLException("Parameter index out of range.");
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (param < 1 || param > inStrings.length)
            throw new SQLException("Parameter index out of range.");
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        if (param < 1 || param > inStrings.length)
            throw new SQLException("Parameter index out of range.");
        if (null == types[param-1]) {
            return Types.NULL;
        } else {
            return types[param-1];
        }
    }

    @Override
    public String getParameterTypeName(int param) {
        return null;
    }

    @Override
    public String getParameterClassName(int param) {
        return null;
    }

    @Override
    public int getParameterMode(int param) {
        return parameterModeUnknown; // == 0
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
