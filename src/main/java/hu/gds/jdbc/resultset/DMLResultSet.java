package hu.gds.jdbc.resultset;

import hu.arheu.gds.message.data.*;
import hu.arheu.gds.message.data.impl.AckStatus;
import hu.arheu.gds.message.data.impl.MessageData2EventImpl;
import hu.gds.jdbc.AttachmentInsertConverter;
import hu.gds.jdbc.DriverPropertyInfoHelper;
import hu.gds.jdbc.GdsBaseStatement;
import hu.gds.jdbc.GdsConnection;
import hu.gds.jdbc.error.ClosedResultSetException;
import hu.gds.jdbc.error.GdsException;
import hu.gds.jdbc.error.InvalidParameterException;
import org.msgpack.value.Value;

import java.sql.SQLException;
import java.util.*;

public class DMLResultSet extends AbstractGdsResultSet {
    private final String queryId;
    private final long mutationCount;

    private List<Boolean> insertedRows;
    private List<Boolean> updatedRows;
    private List<String> tableNames;
    private String tableName;

    private int rowNumber = 0;
    //private final Iterator<DMLResultSet> resultSetIterator;
    private final Iterator<ResultSetWrapper> resultSetIterator;

    private boolean inserted = false;
    private boolean updated = false;

    private ResultSetWrapper currentResultSetWrapper;
    private DMLResultSet currentResultSet;

    public DMLResultSet(Map<String, byte[]> attachments,
                        boolean onlyAttachmentDML,
                        String sql,
                        GdsBaseStatement statement) throws Throwable {
        super(false, sql, statement);
        queryId = UUID.randomUUID().toString();
        setupTimeout(gdsJdbcConnection.getClientInfo(DriverPropertyInfoHelper.TIMEOUT));
        MessageData data;
        if (onlyAttachmentDML) {
            data = AttachmentInsertConverter.getMessageData6AttachmentResponse(sql);
            GdsConnection.OneTimeSyncTransactionExecutor executor =
                    gdsJdbcConnection.getGdsConnection().getNewExecutor(data, queryId, QUERY_TIMEOUT_DEFAULT);
            MessageData7AttachmentResponseAck orphanAttachmentInsertResult = executor.executeAndGetOrphanAttachmentInsertResult();
            if (!AckStatus.OK.equals(orphanAttachmentInsertResult.getGlobalStatus())) {
                mutationCount = 0;
                resultSetIterator = null;
                throw new GdsException("The DML response is not ok: " + orphanAttachmentInsertResult.getGlobalStatus() + " cause: " + orphanAttachmentInsertResult.getGlobalException());
            }
            mutationCount = 1;
            insertedRows = new ArrayList<>();
            insertedRows.add(true);
            updatedRows = new ArrayList<>();
            tableNames = new ArrayList<>();
            tableNames.add(orphanAttachmentInsertResult.getData().getResult().getOwnerTable());
            rows = new ArrayList<>();
            List<FieldHolder> fields = new ArrayList<>();
            List<ResultSetWrapper> resultSets = new ArrayList<>();
            DMLResultSet temp = new DMLResultSet((int) mutationCount, statement, queryId, fields, rows, sql, insertedRows, updatedRows, tableNames);
            resultSets.add(new ResultSetWrapper(temp, temp.rows.size() == 0));
            //resultSets.add(new DMLResultSet((int) mutationCount, statement, queryId, fields, rows, sql, insertedRows, updatedRows, tableNames));
            resultSetIterator = resultSets.iterator();
            nextDMLResultSet();
        } else {
            data = new MessageData2EventImpl(sql, attachments, Collections.emptyList());
            GdsConnection.OneTimeSyncTransactionExecutor executor =
                    gdsJdbcConnection.getGdsConnection().getNewExecutor(data, queryId, QUERY_TIMEOUT_DEFAULT);
            MessageData3EventAck dmlResponse = executor.executeAndGetDmlResult();
            if (!AckStatus.OK.equals(dmlResponse.getGlobalStatus())) {
                mutationCount = 0;
                resultSetIterator = null;
                throw new GdsException("The DML response is not ok: " + dmlResponse.getGlobalStatus() + " cause: " + dmlResponse.getGlobalException());
            }
            int oks = 0;
            List<ResultSetWrapper> resultSets = new ArrayList<>();
            for (EventResultHolder resultHolder : dmlResponse.getEventResult()) {
                int mutated = 0;
                rows = new ArrayList<>();
                List<FieldHolder> fields = resultHolder.getFieldHolders() == null
                        ? new ArrayList<>()
                        : resultHolder.getFieldHolders();
                insertedRows = new ArrayList<>();
                updatedRows = new ArrayList<>();
                tableNames = new ArrayList<>();
                for (EventSubResultHolder eventSubResultHolder : resultHolder.getFieldValues()) {
                    AckStatus status = eventSubResultHolder.getSubStatus();
                    if (AckStatus.OK.equals(status) || AckStatus.ACCEPTED.equals(status)
                            || AckStatus.CREATED.equals(status)) {
                        oks++;
                        mutated++;
                    }
                    if (null != eventSubResultHolder.getRecordValues()
                            && !eventSubResultHolder.getRecordValues().isEmpty()) {
                        rows.add(eventSubResultHolder.getRecordValues());
                    }
                    if (null == eventSubResultHolder.getCreated()) {
                        insertedRows.add(false);
                        updatedRows.add(false);
                    } else {
                        insertedRows.add(eventSubResultHolder.getCreated());
                        updatedRows.add(!eventSubResultHolder.getCreated());
                    }
                    tableNames.add(eventSubResultHolder.getTableName());
                }
                DMLResultSet temp = new DMLResultSet(mutated, statement, queryId, fields, rows, sql, insertedRows, updatedRows, tableNames);
                resultSets.add(new ResultSetWrapper(temp, temp.rows.size() == 0));
            }
            resultSetIterator = resultSets.iterator();
            nextDMLResultSet();
            mutationCount = oks;
        }
    }

    public void addRow(List<Value> row, String tableName, boolean inserted, boolean updated) {
        rows.add(row);
        tableNames.add(tableName);
        insertedRows.add(inserted);
        updatedRows.add(updated);
    }

    @Override
    protected String getTableName() {
        return this.tableName;
    }

    public void nextDMLResultSet() throws SQLException {
        try {
            this.currentResultSetWrapper = resultSetIterator.next();
            this.currentResultSet = currentResultSetWrapper.resultSet().asDmlResultSet();
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

    public ResultSetWrapper getCurrentDMLResultSetWrapper() {
        return currentResultSetWrapper;
    }

    public boolean hasNextDMLResultSet() {
        return resultSetIterator.hasNext();
    }

    public DMLResultSet getCurrentDMLResultSet() {
        return this.currentResultSet;
    }

    private DMLResultSet(int mutated,
                         GdsBaseStatement statement,
                         String queryId,
                         List<FieldHolder> fields,
                         List<List<Value>> rows,
                         String sql,
                         List<Boolean> insertedRows,
                         List<Boolean> updatedRows,
                         List<String> tableNames) throws Throwable {
        super(false, sql, statement);
        this.mutationCount = mutated;
        this.queryId = queryId;
        this.rows = rows;
        resultSetIterator = null;
        List<GdsResultSetMetaData.ColumnMetaData> metaDataList = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            fieldsIndexMap.put(fields.get(i).getFieldName(), i);
            metaDataList.add(ColumnMetaDataHelper.createColumnMetaData(fields.get(i)));
        }
        metaData = new GdsResultSetMetaData(metaDataList, tableName, gdsJdbcConnection);
        this.insertedRows = insertedRows;
        this.updatedRows = updatedRows;
        this.tableNames = tableNames;
    }

    public long getMutationCount() {
        return mutationCount;
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) {
            throw new ClosedResultSetException(sql);
        }
        currentRow = null;
        if (index < rows.size()) {
            currentRow = rows.get(index++);
            rowNumber++;
            inserted = insertedRows.get(index - 1);
            updated = updatedRows.get(index - 1);
            tableName = tableNames.get(index - 1);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLException("Get bytes only allow by attachment response! The request is a standard DML statement and not an attachment query.");
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer col = fieldsIndexMap.get(columnLabel);
        if (null == col) {
            throw new InvalidParameterException("No such column " + columnLabel);
        }
        return col + 1;
    }

    @Override
    public int getRow() {
        return rowNumber;
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosedAndCurrentRow();
        return updated;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosedAndCurrentRow();
        return inserted;
    }

    @Override
    public boolean isDml() {
        return true;
    }

    @Override
    public boolean isDql() {
        return false;
    }

    @Override
    public DQLResultSet asDqlResultSet() throws SQLException {
        throw new SQLException("Its DML result set, not DQL");
    }

    @Override
    public DMLResultSet asDmlResultSet() {
        return this;
    }
}
