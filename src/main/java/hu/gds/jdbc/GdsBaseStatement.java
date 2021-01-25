package hu.gds.jdbc;

import hu.arheu.gds.message.data.ConsistencyType;
import hu.gds.jdbc.executor.DMLExecutor;
import hu.gds.jdbc.executor.DQLExecutor;
import hu.gds.jdbc.resultset.DMLResultSet;
import hu.gds.jdbc.resultset.AbstractGdsResultSet;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.util.concurrent.Queues;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

@SuppressWarnings("RedundantThrows")
public abstract class GdsBaseStatement implements Statement {
    public final static String ATTACHMENT_TABLE_SUFFIX = "-@attachment\"";
    public final static String ATTACHMENT_ID_FIELD = "id";
    public final static String ATTACHMENT_DATA_FIELD = "data";
    protected final boolean isReadOnly;
    protected GdsJdbcConnection connection;
    protected AbstractGdsResultSet dqlOrMultiDmlResultSet;
    protected AbstractGdsResultSet currentResultSet;
    private int fetchSize = Queues.SMALL_BUFFER_SIZE;
    private boolean isClosed = false;
    private int updateCount = -1;
    private final static List<String> allAttachmentFields = Arrays.asList(
            "id",
            "meta",
            "ownerid",
            "data",
            "@ttl",
            "@to_valid");

    /* Vector for batch commands */
    private Vector<String> batch = null;

    GdsBaseStatement(@NotNull GdsJdbcConnection connection) {
        this.connection = connection;
        this.isReadOnly = connection.isReadOnly();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        if (batch == null)
            batch = new Vector<>();
        batch.addElement(sql.trim());
    }

    @Override
    public void clearBatch() throws SQLException {
        if (batch != null)
            batch.removeAllElements();
    }

    @Override
    public void close() throws SQLException {
        synchronized(this) {
            if (isClosed) {
                return;
            }
            this.isClosed = true;
        }
        if (dqlOrMultiDmlResultSet != null) {
            dqlOrMultiDmlResultSet.close();
        }
        if (null != currentResultSet) {
            currentResultSet.close();
        }
        currentResultSet = null;
        dqlOrMultiDmlResultSet = null;
        connection = null;
    }

    void checkClosed(boolean checkResultSet) throws SQLException {
        if (checkResultSet && null == currentResultSet) {
            throw new SQLException("Exhausted ResultSet. There is no current or more result set");
        }
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    protected boolean executeInner(String sql, boolean singleDmlOrDdlStatement, ConsistencyType consistencyType) throws SQLException {
        checkClosed(false);
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            if (singleDmlOrDdlStatement && statements.getStatements().size() > 1) {
                throw new SQLException("Only one statement allowed by this execute there is " + statements.getStatements().size() + " found in the request");
            }
            boolean dqlFound = false;
            boolean dmlFound = false;
            boolean onlyAttachmentDML = true;
            boolean attachmentDQL = false;
            StringBuilder builtSql = new StringBuilder();
            int statementsCounter = statements.getStatements().size();
            Map<String, byte[]> attachments = null;
            final Boolean[] allColumnsSelected = new Boolean[]{false};
            List<String> attachmentSelectFields = new ArrayList<>();
            String selectTableName = null;
            for (int i = 0; i < statementsCounter; i++) {
                net.sf.jsqlparser.statement.Statement statement = statements.getStatements().get(i);
                String table;
                if (statement instanceof Insert) {
                    if (dqlFound) {
                        throw new SQLException("In sql statement not allowed to use SELECT and DML (INSERT, UPDATE, MERGE) statements.");
                    }
                    Insert insert = (Insert) statement;
                    table = insert.getTable().getName();
                    if (null != table && table.endsWith(ATTACHMENT_TABLE_SUFFIX)) {
                        if (null == attachments) {
                            attachments = new HashMap<>();
                        }
                        replaceAttachmentHexBinary(insert, attachments);
                    } else {
                        onlyAttachmentDML = false;
                    }
                    dmlFound = true;
                } else if (statement instanceof Update) {
                    if (dqlFound) {
                        throw new SQLException("In sql statement not allowed to use SELECT and DML (INSERT, UPDATE, MERGE) statements.");
                    }
                    onlyAttachmentDML = false;
                    dmlFound = true;
                } else if (statement instanceof Merge) {
                    if (dqlFound) {
                        throw new SQLException("In sql statement not allowed to use SELECT and DML (INSERT, UPDATE, MERGE) statements.");
                    }
                    onlyAttachmentDML = false;
                    dmlFound = true;
                } else if (statement instanceof Select) {
                    if (singleDmlOrDdlStatement) {
                        throw new SQLException("Select statement found! Not a dml or ddl single statement!");
                    }
                    if (dqlFound) {
                        throw new SQLException("In sql statement multiple SELECT not allowed");
                    }
                    dqlFound = true;
                    if (dmlFound) {
                        throw new SQLException("In sql statement not allowed to use SELECT and DML (INSERT, UPDATE, MERGE) statements.");
                    }
                    onlyAttachmentDML = false;
                    Select select = (Select) statement;
                    final String[] tempTable = new String[1];

                    select.getSelectBody().accept(new SelectDeParser() {

                        @Override
                        public void visit(PlainSelect plainSelect) {
                            plainSelect.accept(new SelectDeParser() {
                                @Override
                                public void visit(Table tableName) {
                                    tempTable[0] = tableName.getName();
                                    tableName.setAlias(null);
                                }

                                @Override
                                public void visit(AllColumns allColumns) {
                                    allColumnsSelected[0] = true;
                                }

                                @Override
                                public void visit(AllTableColumns allTableColumns) {
                                    int index = plainSelect.getSelectItems().indexOf(allTableColumns);
                                    plainSelect.getSelectItems().remove(index);
                                    plainSelect.getSelectItems().add(index, new AllColumns());
                                    allColumnsSelected[0] = true;
                                }

                                @Override
                                public void visit(SelectExpressionItem selectExpressionItem) {
                                    selectExpressionItem.getExpression().accept(new ExpressionDeParser() {
                                        @Override
                                        public void visit(Column tableColumn) {
                                            attachmentSelectFields.add(tableColumn.getColumnName());
                                            tableColumn.setTable(null);
                                        }
                                    });
                                }
                            });
                        }
                    });
                    table = tempTable[0];
                    if (null != table && table.endsWith(ATTACHMENT_TABLE_SUFFIX)) {
                        attachmentDQL = true;
                    }
                    selectTableName = table;
                } else {
                    throw new SQLException("Unsupported statement found: " + statement + ". Only INSERT, UPDATE, MERGE, SELECT allowed");
                }
                builtSql.append(statement).append(";");
                if (i < statementsCounter - 1) {
                    builtSql.append("\n");
                }
            }

            AbstractGdsResultSet resultSet;
            long mutationCount;
            if (dqlFound) {
                DQLExecutor executor = new DQLExecutor(attachmentDQL,
                        connection,
                        builtSql.toString(),
                        selectTableName,
                        allColumnsSelected[0]
                                ? allAttachmentFields
                                : attachmentSelectFields,
                        this,
                        consistencyType);
                resultSet = executor.getResult();
            } else if (dmlFound) {
                DMLExecutor executor = new DMLExecutor(null == attachments ? new HashMap<>() : attachments,
                        onlyAttachmentDML, connection, builtSql.toString(), this);
                resultSet = executor.getResult();
            } else {
                throw new SQLException("The statement is neither a dql nor a dml!");
            }

            mutationCount = setNewResultSet(resultSet);
            return mutationCount == -1;
        } catch (Throwable t) {
            //ha nincs hiba, akkor a régi resultset cserélődik, ha hiba történt, akkor viszont az előzőt bekell zárni --> h2 így működik
            if(currentResultSet != null) {
                currentResultSet.close();
            }
            if(dqlOrMultiDmlResultSet != null) {
                dqlOrMultiDmlResultSet.close();
            }
            throw new SQLException(t);
        }
    }

    private void processOnlyAttachmentDml(Insert insert) {
        String tableName;
        String attachmentId;
        String meta;
        Long ttl;
        Long toValid;
        byte[] attachment;
    }

    private void replaceAttachmentHexBinary(Insert insert, Map<String, byte[]> attachments) throws SQLException {
        List<Column> columns = insert.getColumns();
        if (null == columns || columns.isEmpty()) {
            throw new SQLException("There is no columns in attachment insert", insert.toString(), -1);
        }
        int attachmentDataIndex = -1;
        int attachmentIdIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (ATTACHMENT_ID_FIELD.equals(column.getColumnName())) {
                attachmentIdIndex = i;
            }
            if (ATTACHMENT_DATA_FIELD.equals(column.getColumnName())) {
                attachmentDataIndex = i;
            }
        }
        if (-1 == attachmentIdIndex) {
            throw new SQLException("There is no id coulumn in attachment insert", insert.toString(), -1);
        }
        if (-1 == attachmentDataIndex) {
            throw new SQLException("There is no data coulumn in attachment insert", insert.toString(), -1);
        }
        ItemsList itemsList = insert.getItemsList();
        ExpressionList expressionList = (ExpressionList) itemsList;
        List<Expression> expressions = expressionList.getExpressions();
        String id = ((StringValue) expressions.get(attachmentIdIndex)).getValue();
        String hexValue = ((HexValue) expressions.get(attachmentDataIndex)).getValue();

        byte[] attachment = hexStringToByteArray(hexValue.substring(2));
        String hexId = "0x" + byteArrayToHexString(id.getBytes(StandardCharsets.UTF_8));
        attachments.put(id, attachment);
        expressions.set(attachmentDataIndex, new HexValue(hexId));
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static String byteToHex(byte num) {
        char[] hexDigits = new char[2];
        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
        hexDigits[1] = Character.forDigit((num & 0xF), 16);
        return new String(hexDigits);
    }

    public static String byteArrayToHexString(byte[] byteArray) {
        StringBuilder hexStringBuffer = new StringBuilder();
        for (byte b : byteArray) {
            hexStringBuffer.append(byteToHex(b));
        }
        return hexStringBuffer.toString();
    }

    protected long setNewResultSet(@Nullable AbstractGdsResultSet resultSet) throws SQLException {
        if (null == resultSet) {
            throw new SQLException("Null resultSet");
        }
        if (resultSet != this.dqlOrMultiDmlResultSet
                && null != this.dqlOrMultiDmlResultSet) {
            this.dqlOrMultiDmlResultSet.close();
            this.currentResultSet.close();
        }
        long mutationCount = resultSet.isDql() ? resultSet.asDqlResultSet().getMutationCount() : resultSet.asDmlResultSet().getCurrentDMLResultSet().getMutationCount();
        this.updateCount = coalesceInt(mutationCount);
        this.dqlOrMultiDmlResultSet = resultSet;
        if (resultSet.isDml()) {
            currentResultSet = resultSet.asDmlResultSet().getCurrentDMLResultSet();
        } else {
            currentResultSet = resultSet;
        }
        return mutationCount;
    }

    private int coalesceInt(long value) {
        if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) value;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if(currentResultSet == null) {
            return false;
        }
        checkClosed(false);
        DMLResultSet dmlResultSet;
        if (dqlOrMultiDmlResultSet.isDql()
                || !(dmlResultSet = dqlOrMultiDmlResultSet.asDmlResultSet()).hasNextDMLResultSet()) {
            dqlOrMultiDmlResultSet.close();
            currentResultSet.close();
            return false;
        }
        dmlResultSet.nextDMLResultSet();
        setNewResultSet(dmlResultSet);
        return true;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed(true);
        return updateCount;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed(true);
        return currentResultSet;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (batch == null || batch.isEmpty())
            return new int[0];

        int size = batch.size();
        int[] result = new int[size];
        int i = 0;

        try {
            for (i = 0; i < size; i++)
                result[i] = this.executeUpdate(batch.elementAt(i));
            return result;
        } catch (SQLException e) {
            String message = "Batch failed for request " + i + ": "
                    + batch.elementAt(i) + " (" + e + ")";

            int[] updateCounts = new int[i];
            System.arraycopy(result, 0, updateCounts, 0, i);

            throw new BatchUpdateException(message, updateCounts);
        } finally {
            batch.removeAllElements();
        }
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        //max rows are determined by the GDS and the query page size and query type variables.
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("GDS provides no support for interrupting an operation.");
    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        checkClosed(true);
        // Driver doesn't support positioned updates for now, so no-op.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
