package hu.gds.jdbc.types;

import java.sql.Types;
import java.util.List;
import java.util.Map;

public enum JavaTypes {
    MAP("map", Map.class.getTypeName(), Types.JAVA_OBJECT),
    BINARY("binary", byte[].class.getTypeName(), Types.VARBINARY),
    OBJECT("object", Map.class.getTypeName(), Types.JAVA_OBJECT),
    ARRAY("array", List.class.getTypeName(), Types.ARRAY),
    NUMERIC("numeric", Long.class.getTypeName(), Types.NUMERIC),
    INTEGER("integer", Integer.class.getTypeName(), Types.INTEGER),
    LONG("long", Long.class.getTypeName(), Types.BIGINT),
    BYTE("byte", Byte.class.getTypeName(), Types.TINYINT),
    SHORT("short", Short.class.getTypeName(), Types.INTEGER),
    BOOLEAN("boolean", Boolean.class.getTypeName(), Types.BOOLEAN),
    STRING("string", String.class.getTypeName(), Types.VARCHAR),
    NULL("null", Object.class.getTypeName(), Types.NULL),
    FLOAT("float", Float.class.getTypeName(), Types.FLOAT),
    DOUBLE("double", Double.class.getTypeName(), Types.DOUBLE);

    private final String typeName;
    private final String className;
    private final int sqlType;

    JavaTypes(String typeName, String className, int sqlType) {
        this.typeName = typeName;
        this.className = className;
        this.sqlType = sqlType;
    }

    public int getSqlType() {
        return this.sqlType;
    }

    public String getClassName() {
        return this.className;
    }

    public String getTypeName() {
        return this.typeName;
    }
}
