package hu.gds.jdbc.types;

import hu.gds.jdbc.error.ColumnIndexException;
import hu.gds.jdbc.error.InvalidParameterException;
import hu.gds.jdbc.error.SQLFeatureNotImplemented;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class ArrayImpl implements Array {
    private Object[] array;
    private final JavaTypes arrayItemsType;

    public ArrayImpl(Object[] array, JavaTypes arrayItemsType) {
        this.array = array;
        this.arrayItemsType = arrayItemsType;
    }

    public ArrayImpl(List<?> array, JavaTypes arrayItemsType) {
        this.array = array.toArray();
        this.arrayItemsType = arrayItemsType;
    }

    @Override
    public String getBaseTypeName() {
        return arrayItemsType.getTypeName();
    }

    @Override
    public int getBaseType() {
        return arrayItemsType.getSqlType();
    }

    @Override
    public Object[] getArray() throws SQLException {
        if (array == null) {
            throw new SQLException("Array was freed");
        }
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return getArray(index, count, null);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        Object[] array = getArray();
        if (map != null && !map.isEmpty()) {
            throw new SQLFeatureNotSupportedException();
        }
        if (index < 1 || index > array.length) {
            throw new ColumnIndexException("The column index is out of range: " + index + ", number of columns: " + array.length + ".");
        }
        if (index - 1L + (long) count > (long) array.length) {
            throw new ColumnIndexException("The column index is out of range: " + index + ", count: " + count + ", number of columns: " + array.length + ".");
        }
        if(count <= 0) {
            throw new InvalidParameterException("Invalid count number: " + count + ".");
        }
        return Arrays.copyOfRange(array, (int) index - 1, (int)index - 1 + count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotImplemented();
    }

    @Override
    public void free() {
        if (array != null) {
            array = null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArrayImpl array1 = (ArrayImpl) o;
        return Arrays.equals(array, array1.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }
}
