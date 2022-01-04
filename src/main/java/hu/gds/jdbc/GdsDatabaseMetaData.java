package hu.gds.jdbc;

import hu.arheu.gds.message.data.*;
import hu.arheu.gds.message.data.impl.*;
import hu.gds.jdbc.error.TypeMismatchException;
import hu.gds.jdbc.metainfo.ColumnInfo;
import hu.gds.jdbc.metainfo.GdsTable;
import hu.gds.jdbc.metainfo.GdsTableType;
import hu.gds.jdbc.resultset.DQLResultSet;
import hu.gds.jdbc.resultset.GdsResultSetMetaData;
import hu.gds.jdbc.util.WildcardTranslator;
import org.jetbrains.annotations.NotNull;
import org.msgpack.value.ImmutableNilValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableNilValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;


/**
 * gds namespaces are equivalent to catalogs for this driver. Schemas aren't used. gds indices are
 * equivalent to tables, in that each index is a table.
 */
@SuppressWarnings({"SqlNoDataSourceInspection"})
public class GdsDatabaseMetaData implements DatabaseMetaData {

    private static final String DB_NAME = "GDS";
    private static final String CONFIGURATION_TYPE = "CONFIGURATION";
    private static final String LIST_TYPE = "LIST";
    private static final String TABLE_TYPE = "TABLE";
    private static final String TRANSFORMATION_TYPE = "TRANSFORMATION";

    private static final String GDS_TABLE_NAME_FIELD = "table_name";

    private static final ImmutableNilValue NIL_VALUE = ImmutableNilValueImpl.get();


    private static final List<FieldHolder> SCHEMA_FIELDS =
            new ArrayList<>(Arrays.asList(new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_CATALOG", FieldValueType.TEXT, "")));
    private static final List<FieldHolder> TABLES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_TYPE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SELF_REFERENCING_COL_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("REF_GENERATION", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> COLUMNS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_SIZE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("BUFFER_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DECIMAL_DIGITS", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NUM_PREC_RADIX", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NULLABLE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_DEF", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SQL_DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SQL_DATETIME_SUB", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("CHAR_OCTET_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("ORDINAL_POSITION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("IS_NULLABLE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SCOPE_CATALOG", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SCOPE_SCHEMA", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SCOPE_TABLE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SOURCE_DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("IS_AUTOINCREMENT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("IS_GENERATEDCOLUMN", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> PRIMARY_KEY_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("KEY_SEQ", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("PK_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> INDEX_INFO_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("NON_UNIQUE", FieldValueType.BOOLEAN, ""),
                    new FieldHolderImpl("INDEX_QUALIFIER", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("INDEX_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("ORDINAL_POSITION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("ASC_OR_DESC", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("CARDINALITY", FieldValueType.LONG, ""),
                    new FieldHolderImpl("PAGES", FieldValueType.LONG, ""),
                    new FieldHolderImpl("FILTER_CONDITION", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> TYPE_INFO_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("PRECISION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("LITERAL_PREFIX", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("LITERAL_SUFFIX", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("CREATE_PARAMS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("NULLABLE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("CASE_SENSITIVE", FieldValueType.BOOLEAN, ""),
                    new FieldHolderImpl("SEARCHABLE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("UNSIGNED_ATTRIBUTE", FieldValueType.BOOLEAN, ""),
                    new FieldHolderImpl("FIXED_PREC_SCALE", FieldValueType.BOOLEAN, ""),
                    new FieldHolderImpl("AUTO_INCREMENT", FieldValueType.BOOLEAN, ""),
                    new FieldHolderImpl("LOCAL_TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("MINIMUM_SCALE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("MAXIMUM_SCALE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SQL_DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SQL_DATETIME_SUB", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NUM_PREC_RADIX", FieldValueType.INTEGER, "")
            ));
    private static final List<FieldHolder> PROCEDURES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("PROCEDURE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PROCEDURE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PROCEDURE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PROCEDURE_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SPECIFIC_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> PROCEDURE_COLUMNS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("PROCEDURE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PROCEDURE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PROCEDURE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PRECISION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SCALE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("RADIX", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NULLABLE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_DEF", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SQL_DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SQL_DATETIME_SUB", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("CHAR_OCTET_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("ORDINAL_POSITION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("IS_NULLABLE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SPECIFIC_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> COLUMN_PRIVILEGES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("GRANTOR", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("GRANTEE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PRIVILEGE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("IS_GRANTABLE", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> TABLE_PRIVILEGES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("GRANTOR", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("GRANTEE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PRIVILEGE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("IS_GRANTABLE", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> BEST_ROW_IDENTIFIER_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("SCOPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_SIZE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("BUFFER_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DECIMAL_DIGITS", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("PSEUDO_COLUMN", FieldValueType.INTEGER, "")
            ));
    private static final List<FieldHolder> VERSION_COLUMNS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("SCOPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_SIZE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("BUFFER_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DECIMAL_DIGITS", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("PSEUDO_COLUMN", FieldValueType.INTEGER, "")
            ));
    private static final List<FieldHolder> EXPORTED_KEYS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("PKTABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKTABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKTABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKCOLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKCOLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("KEY_SEQ", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("UPDATE_RULE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DELETE_RULE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("FK_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PK_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DEFERRABILITY", FieldValueType.INTEGER, "")

            ));
    private static final List<FieldHolder> IMPORTED_KEYS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("PKTABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKTABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKTABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKCOLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKCOLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("KEY_SEQ", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("UPDATE_RULE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DELETE_RULE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("FK_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PK_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DEFERRABILITY", FieldValueType.INTEGER, "")

            ));
    private static final List<FieldHolder> CROSS_REFERENCE_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("PKTABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKTABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKTABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PKCOLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKTABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FKCOLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("KEY_SEQ", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("UPDATE_RULE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DELETE_RULE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("FK_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PK_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DEFERRABILITY", FieldValueType.INTEGER, "")
            ));
    private static final List<FieldHolder> UDTS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TYPE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("CLASS_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("BASE_TYPE", FieldValueType.INTEGER, "")
            ));
    private static final List<FieldHolder> SUPER_TYPES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TYPE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SUPERTYPE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SUPERTYPE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SUPERTYPE_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> SUPER_TABLES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TYPE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SUPERTABLE_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> ATTRIBUTES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TYPE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("ATTR_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("ATTR_TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("ATTR_SIZE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DECIMAL_DIGITS", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NUM_PREC_RADIX", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NULLABLE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("ATTR_DEF", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SQL_DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SQL_DATETIME_SUB", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("CHAR_OCTET_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("ORDINAL_POSITION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("IS_NULLABLE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SCOPE_CATALOG", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SCOPE_SCHEMA", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SCOPE_TABLE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SOURCE_DATA_TYPE", FieldValueType.INTEGER, "")
            ));
    private static final List<FieldHolder> CLIENT_INFO_PROPERTIES_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("MAX_LEN", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DEFAULT_VALUE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DESCRIPTION", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> FUNCTIONS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("FUNCTION_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FUNCTION_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FUNCTION_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FUNCTION_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SPECIFIC_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> FUNCTION_COLUMNS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("FUNCTION_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FUNCTION_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("FUNCTION_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("TYPE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("PRECISION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("SCALE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("RADIX", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NULLABLE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("CHAR_OCTET_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("ORDINAL_POSITION", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("IS_NULLABLE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("SPECIFIC_NAME", FieldValueType.TEXT, "")
            ));
    private static final List<FieldHolder> PSEUDO_COLUMNS_FIELDS =
            new ArrayList<>(Arrays.asList(
                    new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_SCHEM", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("TABLE_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("COLUMN_NAME", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("DATA_TYPE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("COLUMN_SIZE", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("DECIMAL_DIGITS", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("NUM_PREC_RADIX", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("COLUMN_USAGE", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("REMARKS", FieldValueType.TEXT, ""),
                    new FieldHolderImpl("CHAR_OCTET_LENGTH", FieldValueType.INTEGER, ""),
                    new FieldHolderImpl("IS_NULLABLE", FieldValueType.TEXT, "")
            ));

    /*
        TABLE_SCHEM String => schema name
        TABLE_CATALOG String => catalog name (may be null)
     */
    private final static MessageData11QueryRequestAck EMPTY_SCHEMAS_RESPONSE =
            createQueryResponse(SCHEMA_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
        REMARKS String => explanatory comment on the table
        TYPE_CAT String => the types catalog (may be null)
        TYPE_SCHEM String => the types schema (may be null)
        TYPE_NAME String => type name (may be null)
        SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
        REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
     */
    private final static MessageData11QueryRequestAck EMPTY_TABLES_RESPONSE =
            createQueryResponse(TABLES_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL type from java.sql.Types
        TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        COLUMN_SIZE int => column size.
        BUFFER_LENGTH is not used.
        DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        NULLABLE int => is NULL allowed.
        columnNoNulls - might not allow NULL values
        columnNullable - definitely allows NULL values
        columnNullableUnknown - nullability unknown
        REMARKS String => comment describing column (may be null)
        COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
        SQL_DATA_TYPE int => unused
        SQL_DATETIME_SUB int => unused
        CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        ORDINAL_POSITION int => index of column in table (starting at 1)
        IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
        YES --- if the column can include NULLs
        NO --- if the column cannot include NULLs
        empty string --- if the nullability for the column is unknown
        SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
        IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
        YES --- if the column is auto incremented
        NO --- if the column is not auto incremented
        empty string --- if it cannot be determined whether the column is auto incremented
        IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
        YES --- if this a generated column
        NO --- if this not a generated column
        empty string --- if it cannot be determined whether this is a generated column
        The COLUMN_SIZE column specifies the column size for the given column. For numeric data, this is the maximum precision. For character data, this is the length in characters. For datetime datatypes, this is the length in characters of the String representation (assuming the maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data types where the column size is not applicable.
     */
    private final static MessageData11QueryRequestAck EMPTY_COLUMNS_RESPONSE =
            createQueryResponse(COLUMNS_FIELDS, new ArrayList<>());
    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
        PK_NAME String => primary key name (may be null)
     */
    private final static MessageData11QueryRequestAck EMPTY_PRIMARY_KEYS_RESPONSE =
            createQueryResponse(PRIMARY_KEY_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
        INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
        INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
        TYPE short => index type:
        tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
        tableIndexClustered - this is a clustered index
        tableIndexHashed - this is a hashed index
        tableIndexOther - this is some other style of index
        ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
        COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
        ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
        CARDINALITY long => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.
        PAGES long => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.
        FILTER_CONDITION String => Filter condition, if any. (may be null)
     */
    private final static MessageData11QueryRequestAck EMPTY_INDEX_INFO_RESPONSE =
            createQueryResponse(INDEX_INFO_FIELDS, new ArrayList<>());

    /*
        TYPE_NAME String => Type name
        DATA_TYPE int => SQL data type from java.sql.Types
        PRECISION int => maximum precision
        LITERAL_PREFIX String => prefix used to quote a literal (may be null)
        LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
        CREATE_PARAMS String => parameters used in creating the type (may be null)
        NULLABLE short => can you use NULL for this type.
        typeNoNulls - does not allow NULL values
        typeNullable - allows NULL values
        typeNullableUnknown - nullability unknown
        CASE_SENSITIVE boolean=> is it case sensitive.
        SEARCHABLE short => can you use "WHERE" based on this type:
        typePredNone - No support
        typePredChar - Only supported with WHERE .. LIKE
        typePredBasic - Supported except for WHERE .. LIKE
        typeSearchable - Supported for all WHERE ..
        UNSIGNED_ATTRIBUTE boolean => is it unsigned.
        FIXED_PREC_SCALE boolean => can it be a money value.
        AUTO_INCREMENT boolean => can it be used for an auto-increment value.
        LOCAL_TYPE_NAME String => localized version of type name (may be null)
        MINIMUM_SCALE short => minimum scale supported
        MAXIMUM_SCALE short => maximum scale supported
        SQL_DATA_TYPE int => unused
        SQL_DATETIME_SUB int => unused
        NUM_PREC_RADIX int => usually 2 or 10
     */
    private final static MessageData11QueryRequestAck EMPTY_TYPE_INFO_RESPONSE =
            createQueryResponse(TYPE_INFO_FIELDS, new ArrayList<>());

    /*
        PROCEDURE_CAT String => procedure catalog (may be null)
        PROCEDURE_SCHEM String => procedure schema (may be null)
        PROCEDURE_NAME String => procedure name
        reserved for future use
        reserved for future use
        reserved for future use
        REMARKS String => explanatory comment on the procedure
        PROCEDURE_TYPE short => kind of procedure:
        procedureResultUnknown - Cannot determine if a return value will be returned
        procedureNoResult - Does not return a return value
        procedureReturnsResult - Returns a return value
        SPECIFIC_NAME String => The name which uniquely identifies this procedure within its schema.
     */
    private final static MessageData11QueryRequestAck EMPTY_PROCEDURES_RESPONSE =
            createQueryResponse(PROCEDURES_FIELDS, new ArrayList<>());

    /*
        PROCEDURE_CAT String => procedure catalog (may be null)
        PROCEDURE_SCHEM String => procedure schema (may be null)
        PROCEDURE_NAME String => procedure name
        COLUMN_NAME String => column/parameter name
        COLUMN_TYPE Short => kind of column/parameter:
        procedureColumnUnknown - nobody knows
        procedureColumnIn - IN parameter
        procedureColumnInOut - INOUT parameter
        procedureColumnOut - OUT parameter
        procedureColumnReturn - procedure return value
        procedureColumnResult - result column in ResultSet
        DATA_TYPE int => SQL type from java.sql.Types
        TYPE_NAME String => SQL type name, for a UDT type the type name is fully qualified
        PRECISION int => precision
        LENGTH int => length in bytes of data
        SCALE short => scale - null is returned for data types where SCALE is not applicable.
        RADIX short => radix
        NULLABLE short => can it contain NULL.
        procedureNoNulls - does not allow NULL values
        procedureNullable - allows NULL values
        procedureNullableUnknown - nullability unknown
        REMARKS String => comment describing parameter/column
        COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
        The string NULL (not enclosed in quotes) - if NULL was specified as the default value
        TRUNCATE (not enclosed in quotes) - if the specified default value cannot be represented without truncation
        NULL - if a default value was not specified
        SQL_DATA_TYPE int => reserved for future use
        SQL_DATETIME_SUB int => reserved for future use
        CHAR_OCTET_LENGTH int => the maximum length of binary and character based columns. For any other datatype the returned value is a NULL
        ORDINAL_POSITION int => the ordinal position, starting from 1, for the input and output parameters for a procedure. A value of 0 is returned if this row describes the procedure's return value. For result set columns, it is the ordinal position of the column in the result set starting from 1. If there are multiple result sets, the column ordinal positions are implementation defined.
        IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
        YES --- if the column can include NULLs
        NO --- if the column cannot include NULLs
        empty string --- if the nullability for the column is unknown
        SPECIFIC_NAME String => the name which uniquely identifies this procedure within its schema.
     */
    private final static MessageData11QueryRequestAck EMPTY_PROCEDURE_COLUMNS_RESPONSE =
            createQueryResponse(PROCEDURE_COLUMNS_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        GRANTOR String => grantor of access (may be null)
        GRANTEE String => grantee of access
        PRIVILEGE String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
        IS_GRANTABLE String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
     */
    private final static MessageData11QueryRequestAck EMPTY_COLUMN_PRIVILEGES_RESPONSE =
            createQueryResponse(COLUMN_PRIVILEGES_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        GRANTOR String => grantor of access (may be null)
        GRANTEE String => grantee of access
        PRIVILEGE String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
        IS_GRANTABLE String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
     */
    private final static MessageData11QueryRequestAck EMPTY_TABLE_PRIVILEGES_RESPONSE =
            createQueryResponse(TABLE_PRIVILEGES_FIELDS, new ArrayList<>());

    /*
        SCOPE short => actual scope of result
        bestRowTemporary - very temporary, while using row
        bestRowTransaction - valid for remainder of current transaction
        bestRowSession - valid for remainder of current session
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL data type from java.sql.Types
        TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        COLUMN_SIZE int => precision
        BUFFER_LENGTH int => not used
        DECIMAL_DIGITS short => scale - Null is returned for data types where DECIMAL_DIGITS is not applicable.
        PSEUDO_COLUMN short => is this a pseudo column like an Oracle ROWID
        bestRowUnknown - may or may not be pseudo column
        bestRowNotPseudo - is NOT a pseudo column
        bestRowPseudo - is a pseudo column
     */
    private final static MessageData11QueryRequestAck EMPTY_BEST_ROW_IDENTIFIER_RESPONSE =
            createQueryResponse(BEST_ROW_IDENTIFIER_FIELDS, new ArrayList<>());

    /*
        SCOPE short => is not used
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL data type from java.sql.Types
        TYPE_NAME String => Data source-dependent type name
        COLUMN_SIZE int => precision
        BUFFER_LENGTH int => length of column value in bytes
        DECIMAL_DIGITS short => scale - Null is returned for data types where DECIMAL_DIGITS is not applicable.
        PSEUDO_COLUMN short => whether this is pseudo column like an Oracle ROWID
        versionColumnUnknown - may or may not be pseudo column
        versionColumnNotPseudo - is NOT a pseudo column
        versionColumnPseudo - is a pseudo column
     */
    private final static MessageData11QueryRequestAck EMPTY_VERSIONS_COLUMNS_RESPONSE =
            createQueryResponse(VERSION_COLUMNS_FIELDS, new ArrayList<>());

    /*
        PKTABLE_CAT String => primary key table catalog (may be null)
        PKTABLE_SCHEM String => primary key table schema (may be null)
        PKTABLE_NAME String => primary key table name
        PKCOLUMN_NAME String => primary key column name
        FKTABLE_CAT String => foreign key table catalog (may be null) being exported (may be null)
        FKTABLE_SCHEM String => foreign key table schema (may be null) being exported (may be null)
        FKTABLE_NAME String => foreign key table name being exported
        FKCOLUMN_NAME String => foreign key column name being exported
        KEY_SEQ short => sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
        UPDATE_RULE short => What happens to foreign key when primary is updated:
        importedNoAction - do not allow update of primary key if it has been imported
        importedKeyCascade - change imported key to agree with primary key update
        importedKeySetNull - change imported key to NULL if its primary key has been updated
        importedKeySetDefault - change imported key to default values if its primary key has been updated
        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
        DELETE_RULE short => What happens to the foreign key when primary is deleted.
        importedKeyNoAction - do not allow delete of primary key if it has been imported
        importedKeyCascade - delete rows that import a deleted key
        importedKeySetNull - change imported key to NULL if its primary key has been deleted
        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
        importedKeySetDefault - change imported key to default if its primary key has been deleted
        FK_NAME String => foreign key name (may be null)
        PK_NAME String => primary key name (may be null)
        DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
        importedKeyInitiallyDeferred - see SQL92 for definition
        importedKeyInitiallyImmediate - see SQL92 for definition
        importedKeyNotDeferrable - see SQL92 for definition
     */
    private final static MessageData11QueryRequestAck EMPTY_EXPORTED_KEYS_RESPONSE =
            createQueryResponse(EXPORTED_KEYS_FIELDS, new ArrayList<>());

    /*
        PKTABLE_CAT String => primary key table catalog being imported (may be null)
        PKTABLE_SCHEM String => primary key table schema being imported (may be null)
        PKTABLE_NAME String => primary key table name being imported
        PKCOLUMN_NAME String => primary key column name being imported
        FKTABLE_CAT String => foreign key table catalog (may be null)
        FKTABLE_SCHEM String => foreign key table schema (may be null)
        FKTABLE_NAME String => foreign key table name
        FKCOLUMN_NAME String => foreign key column name
        KEY_SEQ short => sequence number within a foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
        UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
        importedNoAction - do not allow update of primary key if it has been imported
        importedKeyCascade - change imported key to agree with primary key update
        importedKeySetNull - change imported key to NULL if its primary key has been updated
        importedKeySetDefault - change imported key to default values if its primary key has been updated
        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
        DELETE_RULE short => What happens to the foreign key when primary is deleted.
        importedKeyNoAction - do not allow delete of primary key if it has been imported
        importedKeyCascade - delete rows that import a deleted key
        importedKeySetNull - change imported key to NULL if its primary key has been deleted
        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
        importedKeySetDefault - change imported key to default if its primary key has been deleted
        FK_NAME String => foreign key name (may be null)
        PK_NAME String => primary key name (may be null)
        DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
        importedKeyInitiallyDeferred - see SQL92 for definition
        importedKeyInitiallyImmediate - see SQL92 for definition
        importedKeyNotDeferrable - see SQL92 for definition
     */
    private final static MessageData11QueryRequestAck EMPTY_IMPORTED_KEYS_RESPONSE =
            createQueryResponse(IMPORTED_KEYS_FIELDS, new ArrayList<>());

    /*
        PKTABLE_CAT String => parent key table catalog (may be null)
        PKTABLE_SCHEM String => parent key table schema (may be null)
        PKTABLE_NAME String => parent key table name
        PKCOLUMN_NAME String => parent key column name
        FKTABLE_CAT String => foreign key table catalog (may be null) being exported (may be null)
        FKTABLE_SCHEM String => foreign key table schema (may be null) being exported (may be null)
        FKTABLE_NAME String => foreign key table name being exported
        FKCOLUMN_NAME String => foreign key column name being exported
        KEY_SEQ short => sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
        UPDATE_RULE short => What happens to foreign key when parent key is updated:
        importedNoAction - do not allow update of parent key if it has been imported
        importedKeyCascade - change imported key to agree with parent key update
        importedKeySetNull - change imported key to NULL if its parent key has been updated
        importedKeySetDefault - change imported key to default values if its parent key has been updated
        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
        DELETE_RULE short => What happens to the foreign key when parent key is deleted.
        importedKeyNoAction - do not allow delete of parent key if it has been imported
        importedKeyCascade - delete rows that import a deleted key
        importedKeySetNull - change imported key to NULL if its primary key has been deleted
        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
        importedKeySetDefault - change imported key to default if its parent key has been deleted
        FK_NAME String => foreign key name (may be null)
        PK_NAME String => parent key name (may be null)
        DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
        importedKeyInitiallyDeferred - see SQL92 for definition
        importedKeyInitiallyImmediate - see SQL92 for definition
        importedKeyNotDeferrable - see SQL92 for definition
     */
    private final static MessageData11QueryRequestAck EMPTY_CROSS_REFERENCE_RESPONSE =
            createQueryResponse(CROSS_REFERENCE_FIELDS, new ArrayList<>());

    /*
        TYPE_CAT String => the type's catalog (may be null)
        TYPE_SCHEM String => type's schema (may be null)
        TYPE_NAME String => type name
        CLASS_NAME String => Java class name
        DATA_TYPE int => type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT
        REMARKS String => explanatory comment on the type
        BASE_TYPE short => type code of the source type of a DISTINCT type or the type that implements the user-generated reference type of the SELF_REFERENCING_COLUMN of a structured type as defined in java.sql.Types (null if DATA_TYPE is not DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
     */
    private final static MessageData11QueryRequestAck EMPTY_UDTS_RESPONSE =
            createQueryResponse(UDTS_FIELDS, new ArrayList<>());

    /*
        TYPE_CAT String => the UDT's catalog (may be null)
        TYPE_SCHEM String => UDT's schema (may be null)
        TYPE_NAME String => type name of the UDT
        SUPERTYPE_CAT String => the direct super type's catalog (may be null)
        SUPERTYPE_SCHEM String => the direct super type's schema (may be null)
        SUPERTYPE_NAME String => the direct super type's name
     */
    private final static MessageData11QueryRequestAck EMPTY_SUPER_TYPES_RESPONSE =
            createQueryResponse(SUPER_TYPES_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => the type's catalog (may be null)
        TABLE_SCHEM String => type's schema (may be null)
        TABLE_NAME String => type name
        SUPERTABLE_NAME String => the direct super type's name
     */
    private final static MessageData11QueryRequestAck EMPTY_SUPER_TABLES_RESPONSE =
            createQueryResponse(SUPER_TABLES_FIELDS, new ArrayList<>());

    /*
        TYPE_CAT String => type catalog (may be null)
        TYPE_SCHEM String => type schema (may be null)
        TYPE_NAME String => type name
        ATTR_NAME String => attribute name
        DATA_TYPE int => attribute type SQL type from java.sql.Types
        ATTR_TYPE_NAME String => Data source dependent type name. For a UDT, the type name is fully qualified. For a REF, the type name is fully qualified and represents the target type of the reference type.
        ATTR_SIZE int => column size. For char or date types this is the maximum number of characters; for numeric or decimal types this is precision.
        DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        NULLABLE int => whether NULL is allowed
        attributeNoNulls - might not allow NULL values
        attributeNullable - definitely allows NULL values
        attributeNullableUnknown - nullability unknown
        REMARKS String => comment describing column (may be null)
        ATTR_DEF String => default value (may be null)
        SQL_DATA_TYPE int => unused
        SQL_DATETIME_SUB int => unused
        CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        ORDINAL_POSITION int => index of the attribute in the UDT (starting at 1)
        IS_NULLABLE String => ISO rules are used to determine the nullability for a attribute.
        YES --- if the attribute can include NULLs
        NO --- if the attribute cannot include NULLs
        empty string --- if the nullability for the attribute is unknown
        SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        SCOPE_TABLE String => table name that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type,SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
     */
    private final static MessageData11QueryRequestAck EMPTY_ATTRIBUTES_RESPONSE =
            createQueryResponse(ATTRIBUTES_FIELDS, new ArrayList<>());

    /*
        NAME String=> The name of the client info property
        MAX_LEN int=> The maximum length of the value for the property
        DEFAULT_VALUE String=> The default value of the property
        DESCRIPTION String=> A description of the property. This will typically contain information as to where this property is stored in the database.
     */
    private final static MessageData11QueryRequestAck EMPTY_CLIENT_INFO_PROPERTIES_RESPONSE =
            createQueryResponse(CLIENT_INFO_PROPERTIES_FIELDS, new ArrayList<>());

    /*
        FUNCTION_CAT String => function catalog (may be null)
        FUNCTION_SCHEM String => function schema (may be null)
        FUNCTION_NAME String => function name. This is the name used to invoke the function
        REMARKS String => explanatory comment on the function
        FUNCTION_TYPE short => kind of function:
        functionResultUnknown - Cannot determine if a return value or table will be returned
        functionNoTable- Does not return a table
        functionReturnsTable - Returns a table
        SPECIFIC_NAME String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name that may be different then the FUNCTION_NAME for example with overload functions
     */
    private final static MessageData11QueryRequestAck EMPTY_FUNCTIONS_RESPONSE =
            createQueryResponse(FUNCTIONS_FIELDS, new ArrayList<>());

    /*
        FUNCTION_CAT String => function catalog (may be null)
        FUNCTION_SCHEM String => function schema (may be null)
        FUNCTION_NAME String => function name. This is the name used to invoke the function
        COLUMN_NAME String => column/parameter name
        COLUMN_TYPE Short => kind of column/parameter:
        functionColumnUnknown - nobody knows
        functionColumnIn - IN parameter
        functionColumnInOut - INOUT parameter
        functionColumnOut - OUT parameter
        functionColumnReturn - function return value
        functionColumnResult - Indicates that the parameter or column is a column in the ResultSet
        DATA_TYPE int => SQL type from java.sql.Types
        TYPE_NAME String => SQL type name, for a UDT type the type name is fully qualified
        PRECISION int => precision
        LENGTH int => length in bytes of data
        SCALE short => scale - null is returned for data types where SCALE is not applicable.
        RADIX short => radix
        NULLABLE short => can it contain NULL.
        functionNoNulls - does not allow NULL values
        functionNullable - allows NULL values
        functionNullableUnknown - nullability unknown
        REMARKS String => comment describing column/parameter
        CHAR_OCTET_LENGTH int => the maximum length of binary and character based parameters or columns. For any other datatype the returned value is a NULL
        ORDINAL_POSITION int => the ordinal position, starting from 1, for the input and output parameters. A value of 0 is returned if this row describes the function's return value. For result set columns, it is the ordinal position of the column in the result set starting from 1.
        IS_NULLABLE String => ISO rules are used to determine the nullability for a parameter or column.
        YES --- if the parameter or column can include NULLs
        NO --- if the parameter or column cannot include NULLs
        empty string --- if the nullability for the parameter or column is unknown
        SPECIFIC_NAME String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name that may be different then the FUNCTION_NAME for example with overload functions
     */
    private final static MessageData11QueryRequestAck EMPTY_FUNCTION_COLUMNS_RESPONSE =
            createQueryResponse(FUNCTION_COLUMNS_FIELDS, new ArrayList<>());

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL type from java.sql.Types
        COLUMN_SIZE int => column size.
        DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        COLUMN_USAGE String => The allowed usage for the column. The value returned will correspond to the enum name returned by PseudoColumnUsage.name()
        REMARKS String => comment describing column (may be null)
        CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
        YES --- if the column can include NULLs
        NO --- if the column cannot include NULLs
        empty string --- if the nullability for the column is unknown
     */
    private final static MessageData11QueryRequestAck EMPTY_PSEUDO_COLUMNS_RESPONSE =
            createQueryResponse(PSEUDO_COLUMNS_FIELDS, new ArrayList<>());

    /**
     * All tables stored in the GDS.
     * For a Type many tables can be listed.
     */
    private final static TreeMap<String, GdsTableType> GDS_ALL_TABLE_TYPES = new TreeMap<>();
    /**
     * The name of the TYPES that are special categories (like Config or List) not user tables.
     */
    private final static Set<String> GDS_SYSTEM_TABLE_TYPES = new HashSet<>();
    private final static String SELECT_FROM_GDS_CONFIG_STORE_TABLES = "SELECT * FROM \"@gds.config.store.tables\"";

    /**
     * Every table in the GDS visible by the current user.
     */
    private final TreeMap<String, GdsTable> availableTables = new TreeMap<>();

    static {
        createConfigSystemTables();
        createListSystemTables();
        createTransformationSystemTables();
        createGeneralTables();
    }

    private static void createConfigSystemTables() {
        GdsTableType configTables = new GdsTableType(CONFIGURATION_TYPE, true);
        createPermissionConfigTable(configTables);
        createPermissionConfigValidationTable(configTables);
        createStoreConfigQueryTable(configTables);
        createStoreConfigTablesTable(configTables);
        createCryptoConfigQueryTable(configTables);
        createCryptoConfigGetTable(configTables);
        createAtsConfigTable(configTables);
        createEtsConfigTable(configTables);
        GDS_ALL_TABLE_TYPES.put(CONFIGURATION_TYPE, configTables);
        GDS_SYSTEM_TABLE_TYPES.add(CONFIGURATION_TYPE);
    }

    private static void createListSystemTables() {
        GdsTableType listTables = new GdsTableType(LIST_TYPE, true);
        createListQueryTable(listTables);
        createListQueryDetailsTable(listTables);
        GDS_ALL_TABLE_TYPES.put(LIST_TYPE, listTables);
        GDS_SYSTEM_TABLE_TYPES.add(LIST_TYPE);
    }

    private static void createTransformationSystemTables() {
        GdsTableType transformationTables = new GdsTableType(TRANSFORMATION_TYPE, true);
        createTransformationTable(transformationTables);
        GDS_ALL_TABLE_TYPES.put(TRANSFORMATION_TYPE, transformationTables);
        GDS_SYSTEM_TABLE_TYPES.add(TRANSFORMATION_TYPE);
    }

    private static void createGeneralTables() {
        GdsTableType generalTables = new GdsTableType(TABLE_TYPE, false);
        GDS_ALL_TABLE_TYPES.put(TABLE_TYPE, generalTables);
    }

    private static void createPermissionConfigTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.permission.query", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("version", FieldValueType.KEYWORD, ""), "version", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("is_admin_enabled", FieldValueType.BOOLEAN, ""), "is_admin_enabled", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("config", FieldValueType.TEXT, ""), "config", 3));
        tableHolder.addTable(currentTable);
    }

    private static void createPermissionConfigValidationTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.permission.validation", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("is_valid", FieldValueType.BOOLEAN, ""), "is_valid", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("validation_issues", FieldValueType.TEXT, ""), "validation_issues", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("insert_version", FieldValueType.LONG, ""), "insert_version", 3));
        tableHolder.addTable(currentTable);
    }

    private static void createStoreConfigQueryTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.store.query", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("version", FieldValueType.KEYWORD, ""), "version", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("config", FieldValueType.TEXT, ""), "config", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("pending_change", FieldValueType.BOOLEAN, ""), "pending_change", 3));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("pending_progress", FieldValueType.LONG, ""), "pending_progress", 4));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("pending_status", FieldValueType.TEXT, ""), "pending_status", 5));
        tableHolder.addTable(currentTable);
    }

    private static void createStoreConfigTablesTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.store.tables", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("table_name", FieldValueType.TEXT, ""), "table_name", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("fields", FieldValueType.TEXT, ""), "fields", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("id_field", FieldValueType.TEXT, ""), "id_field", 3));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("id_format", FieldValueType.TEXT, ""), "id_format", 4));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("id_parser", FieldValueType.TEXT, ""), "id_parser", 5));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("fixed", FieldValueType.BOOLEAN, ""), "fixed", 6));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("searchable", FieldValueType.BOOLEAN, ""), "searchable", 7));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("max_query_page_size", FieldValueType.TEXT, ""), "max_query_page_size", 8));
        tableHolder.addTable(currentTable);
    }

    private static void createCryptoConfigQueryTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.crypto.user.query", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("user", FieldValueType.TEXT, ""), "user", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("version", FieldValueType.LONG, ""), "version", 2));
        tableHolder.addTable(currentTable);
    }

    private static void createCryptoConfigGetTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.crypto.user.get", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("config", FieldValueType.TEXT, ""), "config", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("reason", FieldValueType.TEXT, ""), "reason", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("valid", FieldValueType.BOOLEAN, ""), "valid", 3));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("version", FieldValueType.KEYWORD, ""), "version", 4));
        tableHolder.addTable(currentTable);
    }

    private static void createAtsConfigTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.ats.query", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("version", FieldValueType.KEYWORD, ""), "version", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("config", FieldValueType.TEXT, ""), "config", 2));
        tableHolder.addTable(currentTable);
    }

    private static void createEtsConfigTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.ets.query", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("version", FieldValueType.KEYWORD, ""), "version", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("config", FieldValueType.TEXT, ""), "config", 2));
        tableHolder.addTable(currentTable);
    }

    private static void createListQueryTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.list.query", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("blacklist", FieldValueType.TEXT, ""), "blacklist", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("whitelist", FieldValueType.TEXT, ""), "whitelist", 2));
        tableHolder.addTable(currentTable);
    }

    private static void createListQueryDetailsTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.list.query.details", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("blacklist", FieldValueType.TEXT, ""), "blacklist", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("whitelist", FieldValueType.TEXT, ""), "whitelist", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("blacklistid", FieldValueType.TEXT, ""), "blacklistid", 3));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("whitelistid", FieldValueType.TEXT, ""), "whitelistid", 4));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("blacklistheader", FieldValueType.TEXT, ""), "blacklistheader", 5));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("whitelistheader", FieldValueType.TEXT, ""), "whitelistheader", 6));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("blacklistversion", FieldValueType.KEYWORD, ""), "blacklistversion", 7));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("whitelistversion", FieldValueType.KEYWORD, ""), "whitelistversion", 8));
        tableHolder.addTable(currentTable);
    }

    private static void createTransformationTable(GdsTableType tableHolder) {
        GdsTable currentTable = new GdsTable("@gds.config.transformations", tableHolder.getName(), true);
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("class_name", FieldValueType.TEXT, ""), "class_name", 1));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("function_name", FieldValueType.TEXT, ""), "function_name", 2));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("mimetype", FieldValueType.TEXT, ""), "mimetype", 3));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("parameter_count", FieldValueType.LONG, ""), "parameter_count", 4));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("parameter_types", FieldValueType.TEXT_ARRAY, ""), "parameter_types", 5));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("result_types", FieldValueType.TEXT_ARRAY, ""), "result_types", 6));
        currentTable.addColumn(new ColumnInfo(new FieldHolderImpl("description", FieldValueType.TEXT, ""), "description", 7));
        tableHolder.addTable(currentTable);
    }

    public void calculateAvailableTablesAndDescriptors() throws SQLException {
        availableTables.clear();
        for (GdsTableType descriptor : GDS_ALL_TABLE_TYPES.values()) {
            for (Map.Entry<String, GdsTable> entry : descriptor.getTables().entrySet()) {
                GdsTable metaTable = entry.getValue();
                metaTable.setJDBCDescriptor(new ArrayList<>(Arrays.asList(
                        NIL_VALUE, //TABLE_CAT
                        NIL_VALUE, //TABLE_SCHEM
                        new ImmutableStringValueImpl(metaTable.getTableName()), //TABLE_NAME
                        new ImmutableStringValueImpl(entry.getKey()), //TABLE_TYPE
                        new ImmutableStringValueImpl(""), //REMARKS
                        NIL_VALUE, //TYPE_CAT
                        NIL_VALUE, //TYPE_SCHEM
                        NIL_VALUE, //TYPE_NAME
                        NIL_VALUE, //SELF_REFERENCING_COL_NAME
                        NIL_VALUE //REF_GENERATION
                )));
                for (ColumnInfo column : metaTable.getColumnsByOrdinal().values()) {
                    metaTable.addColumn(column);
                }
                availableTables.put(metaTable.getTableName(), metaTable);
            }
        }

        ResultSet resultSet = connection.createStatement().executeQuery(SELECT_FROM_GDS_CONFIG_STORE_TABLES);
        while (resultSet.next()) {
            String tableName = resultSet.getString(GDS_TABLE_NAME_FIELD);
            GdsTable table = new GdsTable(tableName, TABLE_TYPE, false);
            table.setJDBCDescriptor(new ArrayList<>(Arrays.asList(
                    NIL_VALUE, //TABLE_CAT
                    NIL_VALUE, //TABLE_SCHEM
                    new ImmutableStringValueImpl(tableName), //TABLE_NAME
                    new ImmutableStringValueImpl(TABLE_TYPE), //TABLE_TYPE
                    new ImmutableStringValueImpl(""), //REMARKS
                    NIL_VALUE, //TYPE_CAT
                    NIL_VALUE, //TYPE_SCHEM
                    NIL_VALUE, //TYPE_NAME
                    NIL_VALUE, //SELF_REFERENCING_COL_NAME
                    NIL_VALUE //REF_GENERATION
            )));
            String sql = "SELECT * FROM \"@gds.config.store.schema\" WHERE table='" + tableName + "'";
            ResultSet resultSet2 = connection.createStatement().executeQuery(sql);
            Map<String, FieldHolder> fieldHolderMap = new HashMap<>();
            while (resultSet2.next()) {
                String fieldType = resultSet2.getString("field_type");
                String columnName = resultSet2.getString("field_name");
                FieldHolder column = new FieldHolderImpl(columnName, FieldValueType.valueOf(fieldType.toUpperCase()), "");
                fieldHolderMap.put(columnName, column);
            }
            ResultSet orderedColumnSet = connection.createStatement().executeQuery(
                    String.format("SELECT * FROM %1$s LIMIT 0", tableName));
            ResultSetMetaData orderedColumnSetMetaData = orderedColumnSet.getMetaData();
            for (int ii = 1; ii <= orderedColumnSetMetaData.getColumnCount(); ++ii) {
                String columnName = orderedColumnSetMetaData.getColumnName(ii);
                FieldHolder fieldHolderColumn = fieldHolderMap.get(columnName);

                ColumnInfo columnInfo = new ColumnInfo(fieldHolderColumn, columnName, ii);
                columnInfo.setJDBCDescriptor(Arrays.asList(
                        ImmutableNilValueImpl.get(), //1, TABLE_CAT
                        ImmutableNilValueImpl.get(), //2, TABLE_SCHEM
                        new ImmutableStringValueImpl(table.getTableName()), //3, TABLE_NAME
                        new ImmutableStringValueImpl(columnInfo.getColumnName()), //4, COLUMN_NAME
                        new ImmutableLongValueImpl(columnInfo.getSqlType()), //5, DATA_TYPE
                        new ImmutableStringValueImpl(columnInfo.getColumn().getFieldType().toString()), //6, TYPE_NAME
                        new ImmutableLongValueImpl(GdsResultSetMetaData.ColumnMetaData.getDisplaySize(columnInfo.getColumn().getFieldType())), //7, COLUMN_SIZE
                        ImmutableNilValueImpl.get(), //8, BUFFER_LENGTH, nem tudni lehet-e null?
                        ImmutableNilValueImpl.get(), //9, DECIMAL_DIGITS
                        new ImmutableLongValueImpl(10), //10, NUM_PREC_RADIX
                        new ImmutableLongValueImpl(2), //11, NULLABLE (columnNoNulls - might not allow NULL values, columnNullable - definitely allows NULL values, columnNullableUnknown - nullability unknown)
                        ImmutableNilValueImpl.get(), //12, REMARKS
                        ImmutableNilValueImpl.get(), //13, COLUMN_DEF
                        new ImmutableLongValueImpl(columnInfo.getSqlType()), //14, SQL_DATA_TYPE, számok, lehetne valami?
                        ImmutableNilValueImpl.get(), //15, SQL_DATETIME_SUB, számok, lehetne valami
                        new ImmutableLongValueImpl(0), //16, CHAR_OCTET_LENGTH, lehetne a maximum?
                        new ImmutableLongValueImpl(columnInfo.getOrdinalPosition()), //17, ORDINAL_POSITION
                        new ImmutableStringValueImpl(""), //18, IS_NULLABLE
                        ImmutableNilValueImpl.get(), //19, SCOPE_CATALOG
                        ImmutableNilValueImpl.get(), //20, SCOPE_SCHEMA
                        ImmutableNilValueImpl.get(), //21, SCOPE_TABLE
                        ImmutableNilValueImpl.get(), //22, SOURCE_DATA_TYPE
                        new ImmutableStringValueImpl("NO"), //23, IS_AUTOINCREMENT
                        new ImmutableStringValueImpl("NO") //24, IS_GENERATEDCOLUMN
                ));

                table.addColumn(columnInfo);
            }
            availableTables.put(tableName, table);
        }
    }

    private final GdsJdbcConnection connection;
    private final GdsJdbcDriver driver;

    GdsDatabaseMetaData(@NotNull GdsJdbcConnection connection, @NotNull GdsJdbcDriver driver) throws SQLException {
        this.connection = connection;
        this.driver = driver;
        calculateAvailableTablesAndDescriptors();
    }

    private static MessageData11QueryRequestAck createQueryResponse(List<FieldHolder> fieldHolderList, List<List<Value>> rows) {
        try {
            GDSHolder gdsHolder = new GDSHolderImpl("", "");
            QueryContextHolder queryContextHolder = new QueryContextHolderImpl("", "", (long) rows.size(), 0L, ConsistencyType.NONE, "", gdsHolder, new ArrayList<>(), new ArrayList<>());
            QueryResponseHolder queryResponseHolder = new QueryResponseHolderImpl((long) rows.size(), 0L, false, queryContextHolder, fieldHolderList, rows);
            return new MessageData11QueryRequestAckImpl(AckStatus.OK, queryResponseHolder, null);
        } catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        MessageData11QueryRequestAck queryResponse =
                createQueryResponse(new ArrayList<>(Collections.singletonList(new FieldHolderImpl("TABLE_CAT", FieldValueType.TEXT, ""))), new ArrayList<>());
        return new DQLResultSet(queryResponse, "", connection);
    }

    private static final Function<String, Boolean> acceptAll = v -> true;

    /*
     * Későbbre: a pattern lehet SQL-es (LIKE ..) is!
     * */
    private Function<String, Boolean> getPatternChecker(String pattern) {
        if (pattern == null || "%".equals(pattern)) {
            return acceptAll;
        } else {
            Pattern regexpChecker = Pattern.compile(WildcardTranslator.convertPatternToRegex(pattern, null));
            return (name) -> regexpChecker.matcher(name).matches();
        }
    }

    private Set<String> getNeededTypes(String[] types) {
        Set<String> neededTypes = new HashSet<>();
        Set<String> storedTypes = GDS_ALL_TABLE_TYPES.keySet();
        if (types == null) {
            neededTypes.addAll(storedTypes);
        } else {
            for (String type : types) {
                if (storedTypes.contains(type)) {
                    neededTypes.add(type);
                }
            }
        }
        return neededTypes;
    }

    public ResultSet getTables(String catalogName, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if ((null != catalogName && !"".equals(catalogName)) || (null != schemaPattern && !"".equals(schemaPattern))) {
            return new DQLResultSet(EMPTY_TABLES_RESPONSE, "", connection);
        }

        Set<String> neededTypes = getNeededTypes(types);
        if (neededTypes.isEmpty()) {
            return new DQLResultSet(EMPTY_TABLES_RESPONSE, "", connection);
        }

        Function<String, Boolean> patternCheck = getPatternChecker(tableNamePattern);
        List<List<Value>> rows = new ArrayList<>();

        for (GdsTable table : availableTables.values()) {
            if (neededTypes.contains(table.getType()) && patternCheck.apply(table.getTableName())) {
                rows.add(table.getJDBCDescriptor());
            }
        }
        return new DQLResultSet(createQueryResponse(TABLES_FIELDS, rows), "", connection);
    }

    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        if ((null != catalog && !"".equals(catalog)) || (null != schemaPattern && !"".equals(schemaPattern))) {
            return new DQLResultSet(EMPTY_COLUMNS_RESPONSE, "", connection);
        }
        Function<String, Boolean> tableNamePatternCheck = getPatternChecker(tableNamePattern);
        Function<String, Boolean> columnNamePatternCheck = getPatternChecker(columnNamePattern);
        List<List<Value>> rows = new ArrayList<>();
        for (GdsTable table : availableTables.values()) {
            if (tableNamePatternCheck.apply(table.getTableName())) {
                for (ColumnInfo columnInfo : table.getColumnsByOrdinal().values()) {
                    if (columnNamePatternCheck.apply(columnInfo.getColumnName())) {
                        rows.add(columnInfo.getJDBCDescriptor());
                    }
                }
            }
        }
        return new DQLResultSet(createQueryResponse(COLUMNS_FIELDS, rows), "", connection);
    }

    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableName)
            throws SQLException {
        if ((null != catalogName && !"".equals(catalogName)) || (null != schemaName && !"".equals(schemaName)) || null == tableName) {
            return new DQLResultSet(EMPTY_PRIMARY_KEYS_RESPONSE, "", connection);
        }
        String sql = "SELECT * FROM \"@gds.config.store.tables\" WHERE table='" + tableName + "'";

        List<List<Value>> rows = new ArrayList<>();
        ResultSet tables = connection.createStatement().executeQuery(sql);
        while (tables.next()) {
            String id_field = tables.getString("id_field");
            rows.add(new ArrayList<>(Arrays.asList(
                    ImmutableNilValueImpl.get(), //TABLE_CAT
                    ImmutableNilValueImpl.get(), //TABLE_SCHEM
                    new ImmutableStringValueImpl(tableName), //TABLE_NAME
                    new ImmutableStringValueImpl(id_field), //COLUMN_NAME
                    new ImmutableLongValueImpl(1), //KEY_SEQ
                    new ImmutableStringValueImpl(tableName + "." + id_field) //PK_NAME
            )));
        }

        return new DQLResultSet(createQueryResponse(PRIMARY_KEY_FIELDS, rows), "", connection);
    }

    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableName, boolean unique,
                                  boolean approximate) throws SQLException {
        if ((null != catalogName && !"".equals(catalogName)) || (null != schemaName && !"".equals(schemaName)) || null == tableName) {
            return new DQLResultSet(EMPTY_INDEX_INFO_RESPONSE, "", connection);
        }
        String sql = "SELECT * FROM \"@gds.config.store.tables\" WHERE table='" + tableName + "'";

        List<List<Value>> rows = new ArrayList<>();
        ResultSet tables = connection.createStatement().executeQuery(sql);

        while (tables.next()) {
            String id_field = tables.getString("id_field");
            rows.add(new ArrayList<>(Arrays.asList(
                    ImmutableNilValueImpl.get(), //TABLE_CAT String
                    ImmutableNilValueImpl.get(), //TABLE_SCHEM String
                    new ImmutableStringValueImpl(tableName), //TABLE_NAME String
                    ImmutableBooleanValueImpl.FALSE, //NON_UNIQUE boolean
                    ImmutableNilValueImpl.get(), // INDEX_QUALIFIER String
                    new ImmutableStringValueImpl(tableName + "." + id_field), //INDEX_NAME
                    new ImmutableLongValueImpl(2), //TYPE short
                    new ImmutableLongValueImpl(1), //ORDINAL_POSITION short
                    new ImmutableStringValueImpl(id_field), //COLUMN_NAME String
                    new ImmutableStringValueImpl("A"), //ASC_OR_DESC String
                    new ImmutableLongValueImpl(0), //CARDINALITY long ez az összes a rekord száma a db-ben.
                    // Amíg nincs "SELECT COUNT(*) FROM table", addig ezt sem tudjuk
                    new ImmutableLongValueImpl(0), //PAGES long
                    ImmutableNilValueImpl.get() //FILTER_CONDITION String
            )));
        }
        return new DQLResultSet(createQueryResponse(INDEX_INFO_FIELDS, rows), "", connection);
    }

    public ResultSet getTypeInfo() throws SQLException {
        return new DQLResultSet(EMPTY_TYPE_INFO_RESPONSE, "", connection);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new TypeMismatchException("Cannot unwrap to " + iface.getName());
    }

    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    public boolean allProceduresAreCallable() {
        // Standard procedures are not supported by the GDS.
        return false;
    }

    public boolean allTablesAreSelectable() {
        // The GDS uses its internal permission system when the tables are queried with the connected user
        // through the SELECT * FROM "@gds.config.store.tables" statement, this means that only tables that
        // are visible and have at least one visible field are displayed in that list.
        return true;
    }

    public String getURL() {
        return connection.getGdsClientURI().uri;
    }

    public String getUserName() {
        return connection.getGdsClientURI().userName;
    }

    public boolean isReadOnly() {
        return connection.isReadOnly();
    }

    public boolean nullsAreSortedHigh() {
        //Records will be sorted by ID as the last ordering condition, therefore NULLs are not treated separately.
        return false;
    }

    public boolean nullsAreSortedLow() {
        //Records will be sorted by ID as the last ordering condition, therefore NULLs are not treated separately.
        return false;
    }

    public boolean nullsAreSortedAtStart() {
        //Records will be sorted by ID as the last ordering condition, therefore NULLs are not treated separately.
        return false;
    }

    public boolean nullsAreSortedAtEnd() {
        //Records will be sorted by ID as the last ordering condition, therefore NULLs are not treated separately.
        return false;
    }

    public String getDatabaseProductName() {
        return DB_NAME;
    }

    public String getDatabaseProductVersion() {
        //The current GDS Version is "5.2"
        return "5.2";
    }

    public String getDriverName() {
        return "GDS JDBC Driver";
    }

    public String getDriverVersion() {
        return driver.getVersion();
    }

    public int getDriverMajorVersion() {
        return driver.getMajorVersion();
    }

    public int getDriverMinorVersion() {
        return driver.getMinorVersion();
    }

    public boolean usesLocalFiles() {
        //The GDS uses ElasticSearch clusters to store its data
        return false;
    }

    public boolean usesLocalFilePerTable() {
        //The GDS uses ElasticSearch clusters to store its data, not local files
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() {
        // identifiers are case-sensitive
        return true;
    }

    public boolean storesUpperCaseIdentifiers() {
        // identifiers are case-sensitive
        return false;
    }

    public boolean storesLowerCaseIdentifiers() {
        // identifiers are case-sensitive
        return false;
    }

    public boolean storesMixedCaseIdentifiers() {
        // identifiers are case-sensitive
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() {
        // identifiers are case-sensitive
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers() {
        // identifiers are case-sensitive
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() {
        // identifiers are case-sensitive
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() {
        // identifiers are case-sensitive
        return false;
    }

    public String getIdentifierQuoteString() {
        // GDS identifiers are quoted by double quotes.
        return "\"";
    }

    public String getSQLKeywords() {
        // The GDS does not fulfill the requirements of the SQL standard
        return null;
    }

    public String getNumericFunctions() {
        //Open Group CLI functions are not supported;
        return null;
    }

    public String getStringFunctions() {
        //Open Group CLI functions are not supported;
        return null;
    }

    public String getSystemFunctions() {
        //Open Group CLI functions are not supported;
        return null;
    }

    public String getTimeDateFunctions() {
        //Open Group CLI functions are not supported;
        return null;
    }

    public String getSearchStringEscape() {
        // Backslash is used.
        return "\\";
    }

    public String getExtraNameCharacters() {
        // Only the alphanumerical characters and _ can be used.
        return "";
    }

    public boolean supportsAlterTableWithAddColumn() {
        // GDS tables cannot be altered from SQL statements.
        return false;
    }

    public boolean supportsAlterTableWithDropColumn() {
        // GDS tables cannot be altered from SQL statements.
        return false;
    }

    public boolean supportsColumnAliasing() {
        // Aliasing is supported.
        return true;
    }

    public boolean nullPlusNonNullIsNull() {
        // Concatenation is not supported by default
        return false;
    }

    public boolean supportsConvert() {
        // Conversion is not supported
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) {
        // Conversion is not supported
        return false;
    }

    public boolean supportsTableCorrelationNames() {
        // There is no generic correlation support.
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() {
        // There is no generic correlation support.
        return true;
    }

    public boolean supportsExpressionsInOrderBy() {
        // ORDER BY only supports column names
        return false;
    }

    public boolean supportsOrderByUnrelated() {
        // ORDER BY can have columns not in the select
        return true;
    }

    public boolean supportsGroupBy() {
        // GDS has no GROUP BY support yet.
        return false;
    }

    public boolean supportsGroupByUnrelated() {
        // GDS has no GROUP BY support yet.
        return false;
    }

    public boolean supportsGroupByBeyondSelect() {
        // GDS has no GROUP BY support yet.
        return false;
    }

    public boolean supportsLikeEscapeClause() {
        // Escape character of the LIKE expression is predefined.
        return false;
    }

    public boolean supportsMultipleResultSets() {
        // Only one ResultSet is returned
        return false;
    }

    public boolean supportsMultipleTransactions() {
        // GDS does not support transactions.
        return false;
    }

    public boolean supportsNonNullableColumns() {
        // Many columns cannot be null
        return true;
    }

    public boolean supportsMinimumSQLGrammar() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill any of the standard requirements.
        return false;
    }

    public boolean supportsCoreSQLGrammar() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill any of the standard requirements.
        return false;
    }

    public boolean supportsExtendedSQLGrammar() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill any of the standard requirements.
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill any of the standard requirements.
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill any of the standard requirements.
        return false;
    }

    public boolean supportsANSI92FullSQL() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill any of the standard requirements.
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() {
        // Since GDS does not support CREATE / DROP table statements, it does not fulfill the integrity requirements.
        return false;
    }

    public boolean supportsOuterJoins() {
        // The GDS does not support JOIN statements
        return false;
    }

    public boolean supportsFullOuterJoins() {
        // The GDS does not support JOIN statements
        return false;
    }

    public boolean supportsLimitedOuterJoins() {
        // The GDS does not support JOIN statements
        return false;
    }

    public String getSchemaTerm() {
        return "schema";
    }

    public String getProcedureTerm() {
        return "procedure";
    }

    public String getCatalogTerm() {
        return "catalog";
    }

    public boolean isCatalogAtStart() {
        // Catalog names are not used.
        return false;
    }

    public String getCatalogSeparator() {
        return ".";
    }

    public boolean supportsSchemasInDataManipulation() {
        // Schemas are not used / supported
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() {
        // Schemas are not used / supported
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() {
        // Schemas are not used / supported
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() {
        // Schemas are not used / supported
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() {
        // Schemas are not used / supported
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() {
        // Catalogs are not used / supported
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() {
        // Catalogs are not used / supported
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() {
        // Catalogs are not used / supported
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() {
        // Catalogs are not used / supported
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() {
        // Catalogs are not used / supported
        return false;
    }

    public boolean supportsPositionedDelete() {
        // DELETE statements are not supported
        return false;
    }

    public boolean supportsPositionedUpdate() {
        // Since cursors are not supported, positioned updates ar not, either.
        return false;
    }

    public boolean supportsSelectForUpdate() {
        // SELECT FOR UPDATE is not supported.
        return false;
    }

    public boolean supportsStoredProcedures() {
        // standard stored procedures are not supported.
        return false;
    }

    public boolean supportsSubqueriesInComparisons() {
        // The GDS does not support sub queries in general.
        return false;
    }

    public boolean supportsSubqueriesInExists() {
        // The GDS does not support sub queries in general.
        return false;
    }

    public boolean supportsSubqueriesInIns() {
        // The GDS does not support sub queries in general.
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() {
        // The GDS does not support sub queries in general.
        return false;
    }

    public boolean supportsCorrelatedSubqueries() {
        // The GDS does not support sub queries in general.
        return false;
    }

    public boolean supportsUnion() {
        // The GDS does not support UNION expressions for now.
        return false;
    }

    public boolean supportsUnionAll() {
        // The GDS does not support UNION expressions for now.
        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() {
        // There is no transaction support in the GDS.
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() {
        // There is no transaction support in the GDS.
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() {
        // There is no transaction support in the GDS.
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() {
        // There is no transaction support in the GDS.
        return false;
    }

    public int getMaxBinaryLiteralLength() {
        // There is no hard-coded limit
        return 0;
    }

    public int getMaxCharLiteralLength() {
        // There is no hard-coded limit in the ElasticSearch (should fit into an integer probably for string length).
        return 0;
    }

    public int getMaxColumnNameLength() {
        // There is no hard-coded limit in the ElasticSearch (should fit into an integer probably for string length).
        return 0;
    }

    public int getMaxColumnsInGroupBy() {
        // GDS does not support GROUP BY.
        return 0;
    }

    public int getMaxColumnsInIndex() {
        // There is no limit
        return 0;
    }

    public int getMaxColumnsInOrderBy() {
        // There is no limit
        return 0;
    }

    public int getMaxColumnsInSelect() {
        // There is no limit
        return 0;
    }

    public int getMaxColumnsInTable() {
        // There is no limit
        return 0;
    }

    public int getMaxConnections() {
        // GDS does not have any limit on the connections it can serve at the same time
        return 0;
    }

    public int getMaxCursorNameLength() {
        // There is no built-in cursor support
        return 0;
    }

    public int getMaxIndexLength() {
        // NO hard-coded limit.
        return 0;
    }

    public int getMaxSchemaNameLength() {
        // GDS does not have schemas.
        return 0;
    }

    public int getMaxProcedureNameLength() {
        // GDS does not support standard SQL procedures.
        return 0;
    }

    public int getMaxCatalogNameLength() {
        // There are effectively no catalogs in the GDS.
        return 0;
    }

    public int getMaxRowSize() {
        // There is no hardcoded byte limit on the returned rows.
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() {
        // since there is no limit, it "includes" them as well
        return true;
    }

    public int getMaxStatementLength() {
        // There is no hard-coded limit (although the the length of the statement should fit into an integer)
        return 0;
    }

    public int getMaxStatements() {
        // Users can run multiple statements at the same time without any problem
        return 0;
    }

    public int getMaxTableNameLength() {
        //The ElasticSearch only supports a name for an index (table) of maximum 255 bytes.
        //However, since partitioning will add an extra 17 chars to it ("-@gds-yyyyMMdd-xy"), it should be less.
        return 238; // == 255 - 17
    }

    public int getMaxTablesInSelect() {
        // Currently only one table can be used in a SELECT.
        return 1;
    }

    public int getMaxUserNameLength() {
        // While the GDS GUI only allows a username with a max length of 100, it is only for UI/UX purposes.
        return 0;
    }

    public int getDefaultTransactionIsolation() {
        // GDS does not support transactions.
        return Connection.TRANSACTION_NONE;
    }

    public boolean supportsTransactions() {
        // GDS does not support transactions.
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) {
        // GDS does not support transactions.
        return Connection.TRANSACTION_NONE == level;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        // GDS does not support transactions.
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() {
        // GDS does not support transactions.
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() {
        // GDS does not support transactions.
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() {
        // GDS does not support transactions, therefore it ignores everything during (non-existing) transactions.
        return true;
    }

    public ResultSet getProcedures(String catalogName, String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        // GDS does not support the standard "procedure" functionality.
        return new DQLResultSet(EMPTY_PROCEDURES_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getProcedureColumns(String catalogName, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return new DQLResultSet(EMPTY_PROCEDURE_COLUMNS_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<List<Value>> rows = new ArrayList<>();
        for (String key : GDS_ALL_TABLE_TYPES.keySet()) {
            rows.add(Collections.singletonList(new ImmutableStringValueImpl(key)));
        }
        return new DQLResultSet(createQueryResponse(
                new ArrayList<>(Collections.singletonList(
                        new FieldHolderImpl(TABLE_TYPE, FieldValueType.TEXT, "")
                )),
                rows), "", connection);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalogName, String schemaName,
                                         String table, String columnNamePattern) throws SQLException {
        // The GDS uses a different privilege system (PermissionConfig) to determine the user's rights
        return new DQLResultSet(EMPTY_COLUMN_PRIVILEGES_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getTablePrivileges(String catalogName, String schemaPattern, String tableNamePattern) throws
            SQLException {
        // The GDS uses a different privilege system (PermissionConfig) to determine the user's rights
        return new DQLResultSet(EMPTY_TABLE_PRIVILEGES_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalogName, String schemaName, String table, int scope,
                                          boolean nullable) throws SQLException {
        //Each table scheme can have a unique column name as an ID (specified in the scheme as 'id_field').
        return new DQLResultSet(EMPTY_BEST_ROW_IDENTIFIER_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getVersionColumns(String catalogName, String schemaName, String table) throws SQLException {
        //The @@version column does not exist in all tables, therefore we cannot say anything about a version column.
        return new DQLResultSet(EMPTY_VERSIONS_COLUMNS_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getExportedKeys(String catalogName, String schemaName, String tableNamePattern) throws
            SQLException {
        // Since the CREATE TABLE statement is not supported, nor is the FOREIGN KEY keyword and things connecting to it
        return new DQLResultSet(EMPTY_EXPORTED_KEYS_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getImportedKeys(String catalogName, String schemaName, String tableNamePattern) throws
            SQLException {
        // Since the CREATE TABLE statement is not supported, nor is the FOREIGN KEY keyword and things connecting to it
        return new DQLResultSet(EMPTY_IMPORTED_KEYS_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        // Since the CREATE TABLE statement is not supported, nor is the FOREIGN KEY keyword and things connecting to it
        return new DQLResultSet(EMPTY_CROSS_REFERENCE_RESPONSE, "", connection);
    }

    @Override
    public boolean supportsResultSetType(int type) {
        // Users cannot go backwards only forwards
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        // There is no concurrency detection
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        // DELETE is not supported by the GDS.
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        // DELETE is not supported by the GDS.
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        //The AbstractGdsResultSet keeps track of newly inserted rows.
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        // There is no concurrency detection therefore rows are not updated.
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        // DELETE is not supported by the GDS.
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        // There is no concurrency detection therefore rows are not updated.
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        // The AbstractGdsResultSet tracks updates.
        return true;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        // DELETE is not supported by the GDS.
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        // The AbstractGdsResultSet tracks inserts.
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() {
        //multiple updates can be sent in the same event 2 message.
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalogName, String schemaPattern, String typeNamePattern, int[] types) throws
            SQLException {
        // There is no UDT support in the GDS.
        return new DQLResultSet(EMPTY_UDTS_RESPONSE, "", connection);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        // GDS does not support transactions
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        // named parameters are not (yet) supported
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        // only one ResultSet per statement is supported
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        // GDS does not generate keys automatically.
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalogName, String schemaPattern, String typeNamePattern) throws
            SQLException {
        // There are no UDTs in the GDS.
        return new DQLResultSet(EMPTY_SUPER_TYPES_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getSuperTables(String catalogName, String schemaPattern, String tableNamePattern) throws
            SQLException {
        // There is no table hierarchy in the GDS.
        return new DQLResultSet(EMPTY_SUPER_TABLES_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getAttributes(String catalogName, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        // There are no UDTs in the GDS.
        return new DQLResultSet(EMPTY_ATTRIBUTES_RESPONSE, "", connection);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        // GDS does not support transactions
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        // GDS does not support transactions
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[0]);
    }

    @Override
    public int getDatabaseMinorVersion() {
        return Integer.parseInt((getDatabaseProductVersion().split("\\."))[1]);
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateXOpen;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        //Attachments are the only LOBs currently in the GDS, but they cannot be modified like the LOBs generally.
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        // The same statement can be used multiple times
        return true;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalogName, String schemaPattern) throws SQLException {
        return new DQLResultSet(EMPTY_SCHEMAS_RESPONSE, "", connection);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        // GDS does not support the standard "functions" functionality.
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        // GDS does not have transactions.
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // There are no client properties
        return new DQLResultSet(EMPTY_CLIENT_INFO_PROPERTIES_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern) throws
            SQLException {
        // GDS does not support the standard "functions" functionality.
        return new DQLResultSet(EMPTY_FUNCTIONS_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getFunctionColumns(String catalogName, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        // GDS does not support the standard "functions" functionality.
        return new DQLResultSet(EMPTY_FUNCTION_COLUMNS_RESPONSE, "", connection);
    }

    @Override
    public ResultSet getPseudoColumns(String catalogName, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        // The GDS does not have pseudo columns
        return new DQLResultSet(EMPTY_PSEUDO_COLUMNS_RESPONSE, "", connection);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }
}
