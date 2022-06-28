package hu.gds.jdbc.resultset;

import hu.arheu.gds.message.data.FieldHolder;
import hu.arheu.gds.message.data.FieldValueType;
import hu.gds.jdbc.error.InvalidParameterException;
import hu.gds.jdbc.error.TypeMismatchException;
import hu.gds.jdbc.types.JavaTypes;
import hu.gds.jdbc.util.GdsConstants;

import java.sql.SQLException;

public class ColumnMetaDataHelper {
    static GdsResultSetMetaData.ColumnMetaData createColumnMetaData(FieldHolder fieldHolder) throws SQLException {
        String fieldName = fieldHolder.getFieldName();
        JavaTypes type;
        JavaTypes subType;
        switch (fieldHolder.getFieldType()) {
            case KEYWORD, TEXT -> {
                type = JavaTypes.STRING;
                subType = JavaTypes.NULL;
            }
            case KEYWORD_ARRAY, TEXT_ARRAY -> {
                type = JavaTypes.ARRAY;
                subType = JavaTypes.STRING;
            }
            case DOUBLE_ARRAY -> {
                type = JavaTypes.ARRAY;
                subType = JavaTypes.DOUBLE;
            }
            case INTEGER_ARRAY -> {
                type = JavaTypes.ARRAY;
                subType = JavaTypes.INTEGER;
            }
            case LONG_ARRAY -> {
                type = JavaTypes.ARRAY;
                subType = JavaTypes.LONG;
            }
            case BOOLEAN_ARRAY -> {
                type = JavaTypes.ARRAY;
                subType = JavaTypes.BOOLEAN;
            }
            case BOOLEAN -> {
                type = JavaTypes.BOOLEAN;
                subType = JavaTypes.NULL;
            }
            case DOUBLE -> {
                type = JavaTypes.DOUBLE;
                subType = JavaTypes.NULL;
            }
            case INTEGER -> {
                type = JavaTypes.INTEGER;
                subType = JavaTypes.NULL;
            }
            case LONG -> {
                type = JavaTypes.LONG;
                subType = JavaTypes.NULL;
            }
            case BINARY -> {
                type = JavaTypes.BINARY;
                subType = JavaTypes.NULL;
            }
            case BINARY_ARRAY ->
                    throw new TypeMismatchException("By field: " + fieldName + " not allowed type " + fieldHolder.getFieldType() + " found");
            case STRING_MAP -> {
                //"string-map" volt, jó ez így??
                type = JavaTypes.MAP;
                subType = JavaTypes.STRING;
            }
            default ->
                    throw new TypeMismatchException("Found unknown field type. Field name: " + fieldName + " type: " + fieldHolder.getFieldType());
        }
        return GdsResultSetMetaData.createColumn(fieldName, type, subType, fieldHolder.getMimeType(), fieldHolder.getFieldType());
    }

    static GdsResultSetMetaData.ColumnMetaData createAttachmentColumnMetaData(String fieldName) throws SQLException {
        return switch (fieldName) {
            case GdsConstants.ID_FIELD ->
                    GdsResultSetMetaData.createColumn(fieldName, JavaTypes.STRING, JavaTypes.NULL, "", FieldValueType.KEYWORD);
            case GdsConstants.META_FIELD ->
                    GdsResultSetMetaData.createColumn(fieldName, JavaTypes.STRING, JavaTypes.NULL, "", FieldValueType.TEXT);
            case GdsConstants.OWNER_ID_FIELD ->
                    GdsResultSetMetaData.createColumn(fieldName, JavaTypes.ARRAY, JavaTypes.STRING, "", FieldValueType.KEYWORD);
            case GdsConstants.DATA_FIELD ->
                    GdsResultSetMetaData.createColumn(fieldName, JavaTypes.BINARY, JavaTypes.NULL, "", FieldValueType.BINARY);
            case GdsConstants.TTL_FIELD, GdsConstants.TO_VALID_FIELD ->
                    GdsResultSetMetaData.createColumn(fieldName, JavaTypes.LONG, JavaTypes.NULL, "", FieldValueType.LONG);
            default -> throw new InvalidParameterException("Unknown attachment field name: " + fieldName);
        };
    }
}
