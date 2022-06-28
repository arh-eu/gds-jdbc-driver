package hu.gds.jdbc.util;

import org.msgpack.value.Value;
import org.msgpack.value.impl.*;

public class ObjectToValueConverter {

    public static Value convert(Object object) throws Exception {

        if (object instanceof Object[] objects) {
            Value[] values = new Value[objects.length];
            for (int i = 0; i < objects.length; ++i) {
                values[i] = convert(objects[i]);
            }
            return new ImmutableArrayValueImpl(values);
        }

        if (object instanceof byte[]) {
            return new ImmutableBinaryValueImpl((byte[]) object);
        }

        if (object instanceof Boolean) {
            if ((Boolean) object) {
                return ImmutableBooleanValueImpl.TRUE;
            } else {
                return ImmutableBooleanValueImpl.FALSE;
            }
        }

        if (object instanceof Float) {
            return new ImmutableDoubleValueImpl((Float) object);
        }


        if (object instanceof Double) {
            return new ImmutableDoubleValueImpl((Double) object);
        }

        if (object instanceof Integer) {
            return new ImmutableLongValueImpl((Integer) object);
        }

        if (object instanceof Long) {
            return new ImmutableLongValueImpl((Long) object);
        }

        if (object == null) {
            return ImmutableNilValueImpl.get();
        }

        if (object instanceof String) {
            return new ImmutableStringValueImpl((String) object);
        }

        throw new Exception("Unknown type");
    }
}
