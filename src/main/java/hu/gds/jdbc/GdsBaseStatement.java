package hu.gds.jdbc;

import hu.arheu.gds.message.data.ConsistencyType;
import hu.gds.jdbc.error.ClosedResultSetException;
import hu.gds.jdbc.error.ExhaustedResultSetException;
import hu.gds.jdbc.error.InvalidParameterException;
import hu.gds.jdbc.executor.DMLExecutor;
import hu.gds.jdbc.executor.DQLExecutor;
import hu.gds.jdbc.resultset.AbstractGdsResultSet;
import hu.gds.jdbc.resultset.DMLResultSet;
import hu.gds.jdbc.util.GdsConstants;
import hu.gds.jdbc.util.StringEscapeUtils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.InsertDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

@SuppressWarnings("RedundantThrows")
public abstract class GdsBaseStatement implements Statement {
    protected final boolean isReadOnly;
    protected GdsJdbcConnection connection;
    protected AbstractGdsResultSet dqlOrMultiDmlResultSet;
    protected AbstractGdsResultSet currentResultSet;
    private int fetchSize = 256;
    private boolean isClosed = false;
    private int updateCount = -1;
    private int maxRows = 0;
    private final static List<String> allAttachmentFields = Arrays.asList(
            GdsConstants.ID_FIELD,
            GdsConstants.META_FIELD,
            GdsConstants.OWNER_ID_FIELD,
            GdsConstants.DATA_FIELD,
            GdsConstants.TTL_FIELD,
            GdsConstants.TO_VALID_FIELD);

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
        synchronized (this) {
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
            throw new ExhaustedResultSetException("There is no current or more result set");
        }
        if (isClosed) {
            throw new ClosedResultSetException("Statement was previously closed.");
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
                    if (null != table && table.endsWith(GdsConstants.ATTACHMENT_TABLE_SUFFIX)) {
                        if (null == attachments) {
                            attachments = new HashMap<>();
                        }
                        replaceAttachmentHexBinary(insert, attachments);
                    } else {
                        onlyAttachmentDML = false;
                    }
                    List<Column> columns = insert.getColumns();
                    List<Integer> shouldRemove = new ArrayList<>();
                    Integer[] ttl_field_index = new Integer[1];
                    Boolean[] ttl_field_null = new Boolean[1];
                    ttl_field_null[0] = false;
                    for (int j = columns.size() - 1; j >= 0; j--) {
                        if (GdsConstants.isReadOnlyField(StringEscapeUtils.unescapeAtSymbol(columns.get(j).getColumnName()))) {
                            shouldRemove.add(j);
                            columns.remove(j);
                        } else if (GdsConstants.TTL_FIELD.equals(StringEscapeUtils.unescapeAtSymbol(columns.get(j).getColumnName()))) {
                            ttl_field_index[0] = j;
                        }
                    }
                    insert.getItemsList().accept(new InsertDeParser() {
                        @Override
                        public void visit(ExpressionList expressionList) {
                            List<Expression> expressions = expressionList.getExpressions();
                            if (ttl_field_index[0] != null) {
                                expressions.get(ttl_field_index[0]).accept(new ExpressionDeParser() {
                                    @Override
                                    public void visit(NullValue nullValue) {
                                        ttl_field_null[0] = true;
                                    }
                                });
                                if (ttl_field_null[0]) {
                                    expressions.set(ttl_field_index[0], new LongValue(Long.MAX_VALUE));
                                }
                            }
                            for (int i : shouldRemove) {
                                expressions.remove(i);
                            }

                        }
                    });
                    dmlFound = true;
                } else if (statement instanceof Update) {
                    if (dqlFound) {
                        throw new SQLException("In sql statement not allowed to use SELECT and DML (INSERT, UPDATE, MERGE) statements.");
                    }
                    onlyAttachmentDML = false;
                    dmlFound = true;
                    Update update = (Update) statement;
                    Table updateTable = update.getTable();
                    updateTable.setAlias(null);
                    List<Column> columns = update.getColumns();
                    for (Column c : columns) {
                        c.setTable(null);
                        if (GdsConstants.isReadOnlyField(StringEscapeUtils.unescapeAtSymbol(c.getName(false)))) {
                            throw new SQLException("The field " + c.getName(false) + " is not updatable!");
                        }
                    }
                    ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, updateTable.getName());
                    ArrayList<String> keys = new ArrayList<>();
                    while (rs.next()) {
                        keys.add(rs.getString("COLUMN_NAME"));
                    }
                    EqualsTo[] keyExpression = new EqualsTo[1];
                    Boolean[] idLikeExpressionFound = new Boolean[1];
                    idLikeExpressionFound[0] = false;
                    update.getWhere().accept(new ExpressionDeParser() {
                        @Override
                        public void visit(LikeExpression likeExpression) {
                            if (likeExpression.getLeftExpression() instanceof Column) {
                                if (keys.contains(((Column) likeExpression.getLeftExpression()).getColumnName())) {
                                    keyExpression[0] = new EqualsTo();
                                    likeExpression.getLeftExpression().accept(new ExpressionDeParser() {
                                        @Override
                                        public void visit(Column tableColumn) {
                                            tableColumn.setTable(null);
                                        }
                                    });
                                    keyExpression[0].setLeftExpression(likeExpression.getLeftExpression());
                                    keyExpression[0].setRightExpression(likeExpression.getRightExpression());
                                    idLikeExpressionFound[0] = true;
                                }
                            }
                        }
                    });
                    if (idLikeExpressionFound[0]) {
                        update.setWhere(keyExpression[0]);
                    }
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
                            if (maxRows > 0) {
                                Expression e;
                                final long[] originalLimit = new long[1];
                                Limit l = new Limit();
                                if (plainSelect.getLimit() != null) {
                                    e = plainSelect.getLimit().getRowCount();
                                    ExpressionDeParser parser = new ExpressionDeParser() {
                                        @Override
                                        public void visit(LongValue longValue) {
                                            originalLimit[0] = longValue.getValue();
                                        }
                                    };
                                    e.accept(parser);
                                    if (maxRows < originalLimit[0]) {
                                        l.setRowCount(new LongValue(maxRows));
                                    } else {
                                        l.setRowCount(new LongValue(originalLimit[0]));
                                    }
                                } else {
                                    l.setRowCount(new LongValue(maxRows));
                                }
                                plainSelect.setLimit(l);
                            }
                        }
                    });
                    table = tempTable[0];
                    if (null != table && table.endsWith(GdsConstants.ATTACHMENT_TABLE_SUFFIX)) {
                        attachmentDQL = true;
                    }
                    selectTableName = table;
                } else {
                    throw new SQLException("Unsupported statement found: " + statement + ". Only INSERT, UPDATE, MERGE, SELECT are allowed");
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

            if (resultSet.isDml()) {
                return !resultSet.asDmlResultSet().getCurrentDMLResultSetWrapper().isNull();
            } else {
                return mutationCount == -1;
            }
        } catch (Throwable t) {
            //ha nincs hiba, akkor a régi resultset cserélődik, ha hiba történt, akkor viszont az előzőt bekell zárni --> h2 így működik
            if (currentResultSet != null) {
                currentResultSet.close();
            }
            if (dqlOrMultiDmlResultSet != null) {
                dqlOrMultiDmlResultSet.close();
            }
            throw new SQLException(t);
        }
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
            if (GdsConstants.ID_FIELD.equals(column.getColumnName())) {
                attachmentIdIndex = i;
            }
            if (GdsConstants.DATA_FIELD.equals(column.getColumnName())) {
                attachmentDataIndex = i;
            }
        }
        if (-1 == attachmentIdIndex) {
            throw new SQLException("There is no id column in attachment insert", insert.toString(), -1);
        }
        if (-1 == attachmentDataIndex) {
            throw new SQLException("There is no data column in attachment insert", insert.toString(), -1);
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
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        if (resultSet != this.dqlOrMultiDmlResultSet
                && null != this.dqlOrMultiDmlResultSet) {
            this.dqlOrMultiDmlResultSet.close();
            this.currentResultSet.close();
        }
        long mutationCount = resultSet.isDql() ? resultSet.asDqlResultSet().getMutationCount() : resultSet.asDmlResultSet().getCurrentDMLResultSet().getMutationCount();
        this.dqlOrMultiDmlResultSet = resultSet;
        if (resultSet.isDml()) {
            if (resultSet.asDmlResultSet().getCurrentDMLResultSetWrapper().isNull()) {
                currentResultSet = null;
            } else {
                currentResultSet = resultSet.asDmlResultSet().getCurrentDMLResultSet();
            }
        } else {
            currentResultSet = resultSet;
        }
        if (currentResultSet != null) {
            this.updateCount = -1;
        } else {
            this.updateCount = coalesceInt(mutationCount);
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
        checkClosed(false);
        DMLResultSet dmlResultSet;
        if (dqlOrMultiDmlResultSet.isDql()
                || !(dmlResultSet = dqlOrMultiDmlResultSet.asDmlResultSet()).hasNextDMLResultSet()) {
            dqlOrMultiDmlResultSet.close();
            if (currentResultSet != null) {
                currentResultSet.close();
                currentResultSet = null;
            }
            updateCount = -1;
            return false;
        }
        dmlResultSet.nextDMLResultSet();
        setNewResultSet(dmlResultSet);
        return !dmlResultSet.getCurrentDMLResultSetWrapper().isNull();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed(false);
        return updateCount;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed(false);
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
        checkClosed(false);
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed(false);
        if (max < 0) {
            throw new InvalidParameterException("The maxRows value must be non-negative.");
        } else {
            maxRows = max;
        }
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
