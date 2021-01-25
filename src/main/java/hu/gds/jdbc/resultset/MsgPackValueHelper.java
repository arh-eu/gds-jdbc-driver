package hu.gds.jdbc.resultset;

import hu.gds.jdbc.types.ArrayImpl;
import hu.gds.jdbc.types.JavaTypes;
import org.msgpack.value.*;
import org.msgpack.value.impl.ImmutableLongValueImpl;

import java.sql.*;
import java.util.*;

class MsgPackValueHelper {
    private static boolean isNull(Value value) {
        if (null == value || value.isNilValue()) {
            return true;
        }
        return false;
    }

    private static boolean isFit(long value, long min, long max) {
        return value >= min && value <= max;
    }

    private static boolean isFitToByte(long value) {
        return isFit(value, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    private static boolean isFitToShort(long value) {
        return isFit(value, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    private static boolean isFitToInt(long value) {
        return isFit(value, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    private static boolean isFitToLong(long value) {
        return isFit(value, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    private static boolean isFitToFloat(double value) {
        return isFit(value, -Float.MIN_VALUE, Float.MAX_VALUE);
    }

    private static boolean isFitToDouble(double value) {
        return isFit(value, -Double.MIN_VALUE, Double.MAX_VALUE);
    }

    private static boolean isFit(double value, double min, double max) {
        return value >= min && value <= max;
    }

    public static NumberValue getNumberValueFromValue(Value value, JavaTypes targetType) throws SQLException {
        NumberValue numberValue;
        if (isNull(value)) {
            numberValue = new ImmutableLongValueImpl(0);
        } else if (value.isNumberValue()) {
            numberValue = value.asNumberValue();
        } else {
            throw new SQLException("Cannot convert the column of type " + value.getValueType().name() + " to " + targetType + ".");
        }
        boolean isFit;
        switch (targetType) {
            case LONG:
                isFit = true;
                break;
            case DOUBLE:
                isFit = true;
                break;
            case BYTE :
                isFit = isFitToByte(numberValue.toLong());
                break;
            case SHORT:
                isFit = isFitToShort(numberValue.toLong());
                break;
            case INTEGER:
                isFit = isFitToInt(numberValue.toLong());
                break;
            case FLOAT:
                isFit = isFitToFloat(numberValue.toDouble());
                break;
            default:
                throw new SQLException("Cannot convert the column of type " + value.getValueType().name() + " to " + targetType + ".");
        }
        if (!isFit) {
            throw new SQLException("Cannot convert the value " + value.toString() + " to " + targetType);
        }
        return numberValue;
    }

    static boolean getBooleanFromValue(Value value) throws SQLException {
        if (isNull(value)) {
            return false;
        }
        if (value.isBooleanValue()) {
            return value.asBooleanValue().getBoolean();
        }
        if (value.isIntegerValue()) {
            long temp = value.asIntegerValue().asLong();
            if (temp == 0) {
                return false;
            }
            if (temp == 1) {
                return true;
            }
        }
        if (value.isStringValue()) {
            String temp = value.asStringValue().asString();
            if (temp.equals("0")) {
                return false;
            }
            if (temp.equals("1")) {
                return true;
            }
        }
        throw new SQLException("Cannot convert the column of type " + value.getValueType().name() + " to boolean.");
    }

    static String getStringFromValue(Value value, boolean checkType) throws SQLException {
        if (isNull(value)) {
            return null;
        }
        if (value.isStringValue()) {
            return value.asStringValue().asString();
        } else if (checkType) {
            throw new SQLException("Cannot convert the column of type " + value.getValueType().name() + " to string.");
        }
        return value.toString();
    }

    static Array getArrayFromValue(Value value, JavaTypes subType) throws SQLException {
        if (isNull(value)) {
            return null;
        }
        if (!value.isArrayValue()) {
            throw new SQLException("Cannot convert the column of type " + value.getValueType().name() + " to array.");
        }
        List<Object> objectsList = new ArrayList<>();
        List<Value> valuesList = value.asArrayValue().list();
        for (Value item : valuesList) {
            if (item.isStringValue()) {
                objectsList.add(item.asStringValue().asString());
            } else if (item.isNumberValue()) {
                objectsList.add(getObjectFromNumberValue(item.asNumberValue(), subType));
            } else if (item.isIntegerValue()) {
                objectsList.add(item.asIntegerValue().asLong());
            } else if (item.isBooleanValue()) {
                objectsList.add(item.asBooleanValue().getBoolean());
            } else {
                throw new SQLException("Not allowed array item type found: " + item);
            }
        }
        return new ArrayImpl(objectsList, subType);
    }

    private static Object getObjectFromNumberValue(NumberValue value, JavaTypes typeName) throws SQLException {
        switch (typeName) {
            case INTEGER:
                return value.toInt();
            case LONG:
                return value.toLong();
            case DOUBLE:
                return value.toDouble();
            default:
        throw new SQLException("Unknown value type found: " + typeName);
        }
    }

    static Object getObjectFromValue(Value value, JavaTypes typeName, JavaTypes subTypeName) throws SQLException {
        if (isNull(value)) {
            return null;
        } else if (value.isBooleanValue()) {
            return value.asBooleanValue().getBoolean();
        } else if (value.isNumberValue()) {
            return getObjectFromNumberValue(value.asNumberValue(), typeName);
        } else if (value.isStringValue()) {
            return value.asStringValue().asString();
        } else if (value.isArrayValue()) {
            return getArrayFromValue(value, subTypeName);
        } else if (value.isRawValue()) {
            return value.asRawValue().asByteArray();
        }
        return null;
    }

    static Timestamp getTimestampFromValue(Value value, String mimeType) throws SQLException {
        if (isNull(value)) {
            return null;
        }
        if (value.isIntegerValue() && mimeType.equals("datetime")) {
            return new Timestamp(value.asNumberValue().toLong());
        }
        throw new SQLException("Cannot convert the column of type " + value.getValueType().name() + ", mime type \"" + mimeType + "\" to timestamp.");
    }
}
