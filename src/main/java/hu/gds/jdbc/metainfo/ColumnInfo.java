/*
 * Intellectual property of ARH Inc.
 * This file belongs to the GDS 5.1 system in the gds-jdbc project.
 * Budapest, 2020/12/02
 */

package hu.gds.jdbc.metainfo;

import hu.arheu.gds.message.data.FieldHolder;

import java.sql.Types;
import java.util.Objects;

public class ColumnInfo {
    private final FieldHolder column;
    private final String columnName;
    private final int ordinalPosition;

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
        switch (column.getFieldType()) {
            case KEYWORD:
                return Types.VARCHAR;
            case KEYWORD_ARRAY:
            case DOUBLE_ARRAY:
            case INTEGER_ARRAY:
            case BINARY_ARRAY:
            case TEXT_ARRAY:
            case BOOLEAN_ARRAY:
            case LONG_ARRAY:
                return Types.ARRAY;
            case TEXT:
                return Types.LONGVARCHAR;
            case BOOLEAN:
                return Types.BOOLEAN;
            case DOUBLE:
                return Types.DOUBLE;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case BINARY:
                return Types.BINARY;
            case STRING_MAP:
                throw new RuntimeException("Unsupported type: " + column.getFieldType());
            default:
                throw new RuntimeException("Unknown or not implemented gds type found: " + column.getFieldType());
        }
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
}
