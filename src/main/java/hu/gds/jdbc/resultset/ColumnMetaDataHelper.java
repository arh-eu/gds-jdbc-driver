package hu.gds.jdbc.resultset;

import hu.arheu.gds.message.data.FieldHolder;
import hu.arheu.gds.message.data.FieldValueType;
import hu.gds.jdbc.types.JavaTypes;

import java.sql.SQLException;

public class ColumnMetaDataHelper {
    static GdsResultSetMetaData.ColumnMetaData createColumnMetaData(FieldHolder fieldHolder) throws SQLException {
        String fieldName = fieldHolder.getFieldName();
        JavaTypes type;
        JavaTypes subType;
        switch (fieldHolder.getFieldType()) {
            case KEYWORD:
            case TEXT:
                type = JavaTypes.STRING;
                subType = JavaTypes.NULL;
                break;
            case KEYWORD_ARRAY:
                type = JavaTypes.ARRAY;
                subType = JavaTypes.STRING;
                break;
            case DOUBLE_ARRAY:
                type = JavaTypes.ARRAY;
                subType = JavaTypes.DOUBLE;
                break;
            case INTEGER_ARRAY:
                type = JavaTypes.ARRAY;
                subType = JavaTypes.INTEGER;
                break;
            case TEXT_ARRAY:
                type = JavaTypes.ARRAY;
                subType = JavaTypes.STRING;
                break;
            case LONG_ARRAY:
                type = JavaTypes.ARRAY;
                subType = JavaTypes.LONG;
                break;
            case BOOLEAN_ARRAY:
                type = JavaTypes.ARRAY;
                subType = JavaTypes.BOOLEAN;
                break;
            case BOOLEAN:
                type = JavaTypes.BOOLEAN;
                subType = JavaTypes.NULL;
                break;
            case DOUBLE:
                type = JavaTypes.DOUBLE;
                subType = JavaTypes.NULL;
                break;
            case INTEGER:
                type = JavaTypes.INTEGER;
                subType = JavaTypes.NULL;
                break;
            case LONG:
                type = JavaTypes.LONG;
                subType = JavaTypes.NULL;
                break;
            case BINARY:
                type = JavaTypes.BINARY;
                subType = JavaTypes.NULL;
                break;
            case BINARY_ARRAY:
                throw new SQLException("By field: " + fieldName + " not allowed type " + fieldHolder.getFieldType() + " found");
            case STRING_MAP:
                //"string-map" volt, jó ez így??
                type = JavaTypes.MAP;
                subType = JavaTypes.STRING;
                break;
            default:
                throw new SQLException("Found unknown field type. Field name: " + fieldName + " type: " + fieldHolder.getFieldType());
        }
        return GdsResultSetMetaData.createColumn(fieldName, type, subType, fieldHolder.getMimeType(), fieldHolder.getFieldType());
    }

    static GdsResultSetMetaData.ColumnMetaData createAttachmentColumnMetaData(String fieldName) throws SQLException {
        switch (fieldName) {
            case "id":
                return GdsResultSetMetaData. createColumn(fieldName, JavaTypes.STRING, JavaTypes.NULL, "", FieldValueType.KEYWORD);
            case "meta":
                return GdsResultSetMetaData.createColumn(fieldName, JavaTypes.STRING, JavaTypes.NULL, "", FieldValueType.TEXT);
            case "ownerid":
                return GdsResultSetMetaData.createColumn(fieldName, JavaTypes.ARRAY, JavaTypes.STRING, "", FieldValueType.KEYWORD);
            case "data":
                return GdsResultSetMetaData.createColumn(fieldName, JavaTypes.BINARY, JavaTypes.NULL, "", FieldValueType.BINARY);
            case "@ttl":
            case "@to_valid":
                return GdsResultSetMetaData.createColumn(fieldName, JavaTypes.LONG, JavaTypes.NULL, "", FieldValueType.LONG);
            default:
                throw new SQLException("Unknown attachment field name: " + fieldName);
        }
    }
}
