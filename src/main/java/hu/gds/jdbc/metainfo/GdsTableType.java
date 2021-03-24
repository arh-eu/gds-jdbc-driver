/*
 * Intellectual property of ARH Inc.
 * This file belongs to the GDS 5.1 system in the gds-jdbc project.
 * Budapest, 2020/12/02
 */

package hu.gds.jdbc.metainfo;

import java.util.Objects;
import java.util.TreeMap;

public class GdsTableType {
    private final String name;
    private final boolean isSystemTableType;
    private final TreeMap<String, GdsTable> tables;

    public GdsTableType(String name, boolean isSystemTableType) {
        this.name = name;
        this.isSystemTableType = isSystemTableType;
        this.tables = new TreeMap<>();
    }

    public boolean isSystemTableType() {
        return isSystemTableType;
    }

    public String getName() {
        return name;
    }

    public TreeMap<String, GdsTable> getTables() {
        return tables;
    }

    public GdsTable addTable(GdsTable table) {
        return addTable(table.getTableName(), table);
    }

    public GdsTable addTable(String tableName, GdsTable table) {
        return tables.put(tableName, table);
    }

    public GdsTable getTable(String name) {
        return tables.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GdsTableType that = (GdsTableType) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tables);
    }
}
