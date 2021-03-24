/*
 * Intellectual property of ARH Inc.
 * This file belongs to the GDS 5.1 system in the gds-jdbc project.
 * Budapest, 2020/12/02
 */

package hu.gds.jdbc.metainfo;

import org.msgpack.value.Value;

import java.util.*;

public class GdsTable {
    private final String tableName;
    private final TreeMap<String, ColumnInfo> columns;
    private final boolean isVirtualTable;
    private final String tableType;
    private List<Value> JDBCDescriptor;

    public GdsTable(String tableName, String tableType, boolean isVirtualTable) {
        this.tableName = tableName;
        this.tableType = tableType;
        this.columns = new TreeMap<>();
        this.isVirtualTable = isVirtualTable;
        this.JDBCDescriptor = null;
    }

    public boolean isVirtualTable() {
        return isVirtualTable;
    }

    public ColumnInfo addColumn(ColumnInfo info) {
        return addColumn(info.getColumnName(), info);
    }

    public ColumnInfo addColumn(String name, ColumnInfo info) {
        return columns.put(name, info);
    }

    public TreeMap<String, ColumnInfo> getColumns() {
        return columns;
    }

    public ColumnInfo getColumn(String name) {
        return columns.get(name);
    }

    public String getTableName() {
        return tableName;
    }

    public TreeMap<Integer, ColumnInfo> getColumnsByOrdinal() {
        TreeMap<Integer, ColumnInfo> ordinalColumns = new TreeMap<>();
        for (Map.Entry<String, ColumnInfo> column : columns.entrySet()) {
            ordinalColumns.put(column.getValue().getOrdinalPosition(), column.getValue());
        }
        return ordinalColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GdsTable that = (GdsTable) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns);
    }

    public String getType() {
        return tableType;
    }

    public void setJDBCDescriptor(List<Value> value) {
        this.JDBCDescriptor = value;
    }

    public List<Value> getJDBCDescriptor() {
        return JDBCDescriptor;
    }
}
