/*
 * Intellectual property of ARH Inc.
 * This file belongs to the GDS 5.1 system in the gds-jdbc project.
 * Budapest, 2020/12/02
 */

package hu.gds.jdbc.metainfo;

import hu.arheu.gds.message.data.FieldHolder;
import org.msgpack.value.Value;

import java.sql.Types;
import java.util.List;
import java.util.Objects;

public class ColumnInfo {
    private final FieldHolder column;
    private final String columnName;
    private final int ordinalPosition;
    private List<Value> JDBCDescriptor;

    public ColumnInfo(FieldHolder column, String columnName, int ordinalPosition) {
        this.column = column;
        this.columnName = columnName;
        this.ordinalPosition = ordinalPosition;
    }

    public FieldHolder getColumn() {
        return column;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public int getSqlType() {
        return switch (column.getFieldType()) {
            case KEYWORD -> Types.VARCHAR;
            case KEYWORD_ARRAY, DOUBLE_ARRAY, INTEGER_ARRAY, BINARY_ARRAY, TEXT_ARRAY, BOOLEAN_ARRAY, LONG_ARRAY ->
                    Types.ARRAY;
            case TEXT -> Types.LONGVARCHAR;
            case BOOLEAN -> Types.BOOLEAN;
            case DOUBLE -> Types.DOUBLE;
            case INTEGER -> Types.INTEGER;
            case LONG -> Types.BIGINT;
            case BINARY -> Types.BINARY;
            case STRING_MAP -> throw new RuntimeException("Unsupported type: " + column.getFieldType());
            default ->
                    throw new RuntimeException("Unknown or not implemented gds type found: " + column.getFieldType());
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnInfo that = (ColumnInfo) o;
        return ordinalPosition == that.ordinalPosition &&
                Objects.equals(column, that.column) &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, columnName, ordinalPosition);
    }

    public void setJDBCDescriptor(List<Value> value) {
        this.JDBCDescriptor = value;
    }

    public List<Value> getJDBCDescriptor() {
        return JDBCDescriptor;
    }
}
