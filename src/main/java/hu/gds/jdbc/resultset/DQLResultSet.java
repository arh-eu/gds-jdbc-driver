package hu.gds.jdbc.resultset;

import hu.arheu.gds.message.data.*;
import hu.arheu.gds.message.data.impl.AckStatus;
import hu.arheu.gds.message.data.impl.MessageData10QueryRequestImpl;
import hu.arheu.gds.message.data.impl.MessageData12NextQueryPageImpl;
import hu.arheu.gds.message.data.impl.MessageData4AttachmentRequestImpl;
import hu.gds.jdbc.DriverPropertyInfoHelper;
import hu.gds.jdbc.GdsBaseStatement;
import hu.gds.jdbc.GdsConnection;
import hu.gds.jdbc.GdsJdbcConnection;
import hu.gds.jdbc.error.ClosedResultSetException;
import hu.gds.jdbc.error.GdsException;
import hu.gds.jdbc.error.InvalidParameterException;
import hu.gds.jdbc.util.GdsConstants;
import org.msgpack.value.Value;
import org.msgpack.value.impl.*;

import java.sql.SQLException;
import java.util.*;

import static hu.gds.jdbc.resultset.GdsResultSetMetaData.ColumnMetaData;

public class DQLResultSet extends AbstractGdsResultSet {

    private int QUERY_PAGE_SIZE;
    private int QUERY_TYPE;
    private int PREFETCH_NUMBER;

    private final static int MAX_RANDOM_DELAY_BETWEEN_ERROR = 1000;

    private enum PrefetchState {
        IDLING,
        IN_PROGRESS
    }

    //private final boolean attachmentDQL;
    private String sql;

    MessageData11QueryRequestAck queryResponse;
    MessageData attachmentResponse;
    AttachmentResultHolder attachmentResultHolder;
    private final Object lock = new Object();
    private final Deque<MessageData11QueryRequestAck> prefechedResponses = new ArrayDeque<>();
    private PrefetchState state = PrefetchState.IDLING;
    private int retryOnError;

    private String tableName;

    public DQLResultSet(MessageData11QueryRequestAck queryResponse,
                        String sql,
                        GdsJdbcConnection connection) throws SQLException {
        super(false, sql, connection);
        init(queryResponse, sql);
    }

    public DQLResultSet(MessageData11QueryRequestAck queryResponse,
                        String sql,
                        GdsBaseStatement statement) throws SQLException {
        super(false, sql, statement);
        init(queryResponse, sql);
    }

    private void init(MessageData11QueryRequestAck queryResponse,
                      String sql) throws SQLException {
        this.queryResponse = queryResponse;
        this.metaData = initNotAttachmentDql(queryResponse);
        this.queryId = UUID.randomUUID().toString();
        this.sql = sql;
        this.retryOnError = (int) longValueFromString(gdsJdbcConnection.getClientInfo(DriverPropertyInfoHelper.RETRY_LIMIT_ON_ERROR),
                "retryOnError", RETRY_ON_ERROR_DEFAULT);

        this.QUERY_TYPE = (int) longValueFromString(gdsJdbcConnection.getClientInfo(DriverPropertyInfoHelper.DQL_QUERY_TYPE),
                "QUERY_TYPE", 0);

        this.QUERY_PAGE_SIZE = (int) longValueFromString(gdsJdbcConnection.getClientInfo(DriverPropertyInfoHelper.DQL_QUERY_PAGE_SIZE),
                "QUERY_PAGE_SIZE", 100);

        this.PREFETCH_NUMBER = (int) longValueFromString(gdsJdbcConnection.getClientInfo(DriverPropertyInfoHelper.PREFETCH),
                "PREFETCH_NUMBER", 3);
    }


    public void addRow(List<Value> row) {
        this.rows.add(row);
    }

    public DQLResultSet(boolean attachmentDQL,
                        String sql,
                        String tableName,
                        List<String> attachmentSelectedFields,
                        GdsBaseStatement statement,
                        ConsistencyType consistencyType) throws Throwable {
        super(attachmentDQL, sql, statement);
        //this.connection = connection;
        this.sql = sql;
        this.tableName = tableName;
        queryId = UUID.randomUUID().toString();
        setupTimeout(gdsJdbcConnection.getClientInfo(DriverPropertyInfoHelper.TIMEOUT));

        MessageData data;
        if (attachmentDQL) {
            data = new MessageData4AttachmentRequestImpl(sql);
            GdsConnection.OneTimeSyncTransactionExecutor executor =
                    gdsJdbcConnection.getGdsConnection().getNewExecutor(data, queryId, timeout);
            attachmentResponse = executor.executeAndGetAttachmentQueryResult();
            if (attachmentResponse.isAttachmentRequestAckMessageData5()) {
                MessageData5AttachmentRequestAck ack =
                        attachmentResponse.asAttachmentRequestAckMessageData5();
                if (!AckStatus.OK.equals(ack.getGlobalStatus())) {
                    throw new GdsException("The query response is not ok: " + ack.getGlobalStatus());
                }
                attachmentResultHolder = ack.getData().getResult();
            } else {
                MessageData6AttachmentResponse data6AttachmentResponse =
                        attachmentResponse.asAttachmentResponseMessageData6();
                attachmentResultHolder = data6AttachmentResponse.getResult();
            }
            List<ColumnMetaData> metaDataList = new ArrayList<>();
            for (int i = 0; i < attachmentSelectedFields.size(); i++) {
                String attachmentField = attachmentSelectedFields.get(i);
                fieldsIndexMap.put(attachmentField, i + 1);
                metaDataList.add(ColumnMetaDataHelper.createAttachmentColumnMetaData(attachmentField));
            }
            metaData = new GdsResultSetMetaData(metaDataList, tableName, gdsJdbcConnection);
        } else {
            data = new MessageData10QueryRequestImpl(
                    sql,
                    consistencyType,
                    timeout,
                    QUERY_PAGE_SIZE,
                    QUERY_TYPE);
            GdsConnection.OneTimeSyncTransactionExecutor executor =
                    gdsJdbcConnection.getGdsConnection().getNewExecutor(data, queryId, timeout);
            queryResponse = executor.executeAndGetQueryResult();
            metaData = initNotAttachmentDql(queryResponse);
            if (queryResponse.getQueryResponseHolder().getMorePage()) {
                synchronized (lock) {
                    state = PrefetchState.IN_PROGRESS;
                    doPrefetch(retryOnError, queryResponse.getQueryResponseHolder().getQueryContextHolder());
                }
            }
        }
    }

    /*
        Van egy olyan, amit fel lehetne használni?
     */
    private GdsResultSetMetaData initNotAttachmentDql(MessageData11QueryRequestAck queryResponse) throws SQLException {
        checkResponseAndSetRows(queryResponse);
        List<FieldHolder> fields = queryResponse.getQueryResponseHolder().getFieldHolders();
        return new GdsResultSetMetaData(getMetaDataList(fields), tableName, gdsJdbcConnection);
    }

    private void checkResponseAndSetRows(MessageData11QueryRequestAck queryResponse) throws SQLException {
        checkQueryResponse(queryResponse);
        setRows(queryResponse);
    }

    private void checkQueryResponse(MessageData11QueryRequestAck queryResponse) throws SQLException {
        if (!AckStatus.OK.equals(queryResponse.getGlobalStatus())) {
            throw new GdsException("The query response is not ok: " + queryResponse.getGlobalStatus() + ", message: " + queryResponse.getGlobalException());
        }
    }

    private void setRows(MessageData11QueryRequestAck queryResponse) {
        rows = queryResponse.getQueryResponseHolder().getHits();
        index = 0;
    }

    private List<ColumnMetaData> getMetaDataList(List<FieldHolder> fields) throws SQLException {
        List<ColumnMetaData> metaDataList = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            fieldsIndexMap.put(fields.get(i).getFieldName(), i + 1);
            metaDataList.add(ColumnMetaDataHelper.createColumnMetaData(fields.get(i)));
        }
        return metaDataList;
    }

    public long getMutationCount() {
        return -1;
    }

    private boolean setAttachmentNextCurrentRow() {
        if (0 == index) {
            index++;
            rowNumber++;
        } else {
            return false;
        }
        currentRow = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fieldsIndexMap.entrySet()) {
            String attachmentField = entry.getKey();
            switch (attachmentField) {
                case GdsConstants.ID_FIELD ->
                        currentRow.add(new ImmutableStringValueImpl(attachmentResultHolder.getAttachmentId()));
                case GdsConstants.META_FIELD -> {
                    String meta = attachmentResultHolder.getMeta();
                    currentRow.add(meta == null ?
                            ImmutableNilValueImpl.get()
                            : new ImmutableStringValueImpl(meta));
                }
                case GdsConstants.OWNER_ID_FIELD -> {
                    Value[] array = new Value[attachmentResultHolder.getOwnerIds().size()];
                    for (int i = 0; i < attachmentResultHolder.getOwnerIds().size(); i++) {
                        array[i] = new ImmutableStringValueImpl(attachmentResultHolder.getOwnerIds().get(i));
                    }
                    currentRow.add(new ImmutableArrayValueImpl(array));
                }
                case GdsConstants.DATA_FIELD ->
                        currentRow.add(new ImmutableBinaryValueImpl(attachmentResultHolder.getAttachment()));
                case GdsConstants.TTL_FIELD ->
                        currentRow.add(new ImmutableLongValueImpl(attachmentResultHolder.getTtl()));
                case GdsConstants.TO_VALID_FIELD ->
                        currentRow.add(new ImmutableLongValueImpl(attachmentResultHolder.getToValid()));
            }
        }
        return true;
    }

    private boolean setNextCurrentRow() throws SQLException {
        while (true) {
            if (index < rows.size()) {
                currentRow = rows.get(index++);
                rowNumber++;
                return true;
            } else if (queryResponse.getQueryResponseHolder().getMorePage()) {
                makeNextQuery();
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) {
            throw new ClosedResultSetException(sql);
        }
        currentRow = null;
        if (attachmentDQL) {
            return setAttachmentNextCurrentRow();
        } else {
            return setNextCurrentRow();
        }
    }

    /*
        teljesen mindegy, milyen okból van meghívva.
        Még van, akár le kell kérdezni, akár a
        queue-ban van
     */
    private void makeNextQuery() throws SQLException {
        try {
            MessageData11QueryRequestAck nextQueryResponse = null;
            boolean prefetch = false;
            QueryContextHolder queryContextHolder = null;
            if (0 < PREFETCH_NUMBER) {
                synchronized (lock) {
                    if (prefechedResponses.isEmpty()
                            && PrefetchState.IN_PROGRESS.equals(state)) {
                        lock.wait();
                    }
                    if (PrefetchState.IDLING.equals(state)) {
                        MessageData11QueryRequestAck prefetchedLastElement = prefechedResponses.peekLast();
                        if (null == prefetchedLastElement) {
                            prefetchedLastElement = queryResponse;
                        }
                        if (prefetchedLastElement.getQueryResponseHolder().getMorePage()) {
                            prefetch = true;
                            queryContextHolder = prefetchedLastElement.getQueryResponseHolder().getQueryContextHolder();
                            state = PrefetchState.IN_PROGRESS;
                        }
                    }
                    nextQueryResponse = prefechedResponses.poll();
                }
            }
            if (null != nextQueryResponse) {
                queryResponse = nextQueryResponse;
                setRows(queryResponse);
            } else {
                int tryout = retryOnError;
                while (true) {
                    try {
                        queryResponse = makeNextQuery(queryResponse.getQueryResponseHolder().getQueryContextHolder());
                        checkResponseAndSetRows(queryResponse);
                        if (prefetch) {
                            if (!queryResponse.getQueryResponseHolder().getMorePage()) {
                                prefetch = false;
                                synchronized (lock) {
                                    state = PrefetchState.IDLING;
                                    lock.notifyAll();
                                }
                            } else {
                                queryContextHolder = queryResponse.getQueryResponseHolder().getQueryContextHolder();
                            }
                        }
                        break;
                    } catch (Throwable ex) {
                        if (0 >= --tryout
                                || gdsJdbcConnection.getGdsConnection().isDisconnected()) {
                            if (prefetch) {
                                synchronized (lock) {
                                    state = PrefetchState.IDLING;
                                    lock.notifyAll();
                                }
                            }
                            throw ex;
                        } else {
                            Random random = new Random();
                            try {
                                //noinspection BusyWait
                                Thread.sleep(random.nextInt(MAX_RANDOM_DELAY_BETWEEN_ERROR));
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }
            }
            if (prefetch) {
                doPrefetch(retryOnError, queryContextHolder);
            }
        } catch (Throwable ex) {
            throw new GdsException("Error while execute sql", ex);
        }
    }

    private MessageData11QueryRequestAck makeNextQuery(QueryContextHolder queryContextHolder) throws Throwable {
        String queryId = UUID.randomUUID().toString();
        MessageData12NextQueryPageImpl data = new MessageData12NextQueryPageImpl(queryContextHolder, timeout);
        GdsConnection.OneTimeSyncTransactionExecutor executor = gdsJdbcConnection.getGdsConnection().getNewExecutor(data, queryId, timeout);
        return executor.executeAndGetQueryResult();
    }

    /*
        Kizárólagos jogban fut
        notify-t végez,
          ha egyet sikerült lekérnie,
          ha megállt, mert mindent lekérdezett
          ha hibával ért véget, a tryoutok száma miatt.
     */
    private void doPrefetch(int tryout, QueryContextHolder queryContextHolder) {
        new Thread(() -> {
            try {
                MessageData11QueryRequestAck nextPrefetchedQueryResponse = makeNextQuery(queryContextHolder);
                checkQueryResponse(nextPrefetchedQueryResponse);
                boolean prefetch = false;
                synchronized (lock) {
                    prefechedResponses.add(nextPrefetchedQueryResponse);
                    if (PREFETCH_NUMBER <= prefechedResponses.size()) {
                        state = PrefetchState.IDLING;
                    } else if (nextPrefetchedQueryResponse.getQueryResponseHolder().getMorePage()) {
                        prefetch = true;
                    }
                    lock.notifyAll();
                }
                if (prefetch) {
                    doPrefetch(tryout, nextPrefetchedQueryResponse.getQueryResponseHolder().getQueryContextHolder());
                }
            } catch (Throwable ex) {
                if (0 >= tryout
                        || gdsJdbcConnection.getGdsConnection().isDisconnected()) {
                    synchronized (lock) {
                        state = PrefetchState.IDLING;
                        lock.notifyAll();
                    }
                } else {
                    Random random = new Random();
                    try {
                        Thread.sleep(random.nextInt(MAX_RANDOM_DELAY_BETWEEN_ERROR));
                    } catch (InterruptedException ignored) {
                    }
                    doPrefetch(tryout - 1, queryContextHolder);
                }
            }
        }).start();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer col = fieldsIndexMap.get(columnLabel);
        if (null == col) {
            throw new InvalidParameterException("No such column " + columnLabel);
        }
        return col;
    }

    @Override
    public int getRow() {
        return rowNumber;
    }

    @Override
    public int getFetchSize() {
        return QUERY_PAGE_SIZE;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean isDml() {
        return false;
    }

    @Override
    public boolean isDql() {
        return true;
    }

    @Override
    public DQLResultSet asDqlResultSet() {
        return this;
    }

    @Override
    public DMLResultSet asDmlResultSet() throws SQLException {
        throw new SQLException("Its DQL result set, not DML");
    }

    @Override
    protected String getTableName() {
        return this.tableName;
    }
}
