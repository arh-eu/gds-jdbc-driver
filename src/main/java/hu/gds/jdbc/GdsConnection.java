package hu.gds.jdbc;

import hu.arheu.gds.client.AsyncGDSClient;
import hu.arheu.gds.client.Either;
import hu.arheu.gds.client.GDSMessageListener;
import hu.arheu.gds.client.Pair;
import hu.arheu.gds.message.data.*;
import hu.arheu.gds.message.data.impl.AckStatus;
import hu.arheu.gds.message.data.impl.AttachmentResponseAckResultHolderImpl;
import hu.arheu.gds.message.data.impl.AttachmentResultHolderImpl;
import hu.arheu.gds.message.data.impl.MessageData7AttachmentResponseAckImpl;
import hu.arheu.gds.message.header.MessageHeader;
import hu.arheu.gds.message.header.MessageHeaderBase;
import hu.gds.jdbc.error.GdsException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * This class holds and manages the connection towards the GDS.
 */
public class GdsConnection {
    private final AsyncGDSClient client;
    private boolean connected = false;
    private boolean inited = false;
    private boolean initialized = false;
    private boolean closed = false;
    private String reason = null;
    private final Object lock = new Object();
    private final Set<OneTimeSyncTransactionExecutor> executors = ConcurrentHashMap.newKeySet(); //Thread safe concurrent set

    private static final Logger LOG = Logger.getLogger(GdsConnection.class.getName());

    public class OneTimeSyncTransactionExecutor {
        private final MessageData requestData;
        private final String requestIdToWaitFor;
        private final long timeoutMillis;
        private final Object executorLock = new Object();
        private MessageData result = null;
        private Throwable cause = null;

        public OneTimeSyncTransactionExecutor(MessageData requestData,
                                              String requestIdToWaitFor,
                                              long timeoutMillis) throws Throwable {
            this.requestData = requestData;
            this.requestIdToWaitFor = requestIdToWaitFor;
            this.timeoutMillis = timeoutMillis;
            sendRequest();
        }

        /**
         * Run the query, and wait for complete the query or timeout the request
         *
         * @return The query result for 10 and 12 query request
         */
        public MessageData11QueryRequestAck executeAndGetQueryResult() throws Throwable {
            return getResult().asQueryRequestAckMessageData11();
        }

        /**
         * Run the DML event, and wait for complete the event or timeout the request
         *
         * @return The DML result for 2 event request
         */
        public MessageData3EventAck executeAndGetDmlResult() throws Throwable {
            return getResult().asEventAckMessageData3();
        }

        public MessageData7AttachmentResponseAck executeAndGetOrphanAttachmentInsertResult() throws Throwable {
            return getResult().asAttachmentResponseAckMessageData7();
        }

        private MessageData getResult() throws Throwable {
            synchronized (executorLock) {
                if (null != result) {
                    executors.remove(this);
                    return result;
                }
                if (null != cause) {
                    executors.remove(this);
                    throw cause;
                }
                executorLock.wait(timeoutMillis);
                executors.remove(this);
                if (null != cause) {
                    throw cause;
                }
                if (null == result) {
                    throw new TimeoutException("the query with request id: " + requestIdToWaitFor + " timed out");
                }
                return result;
            }
        }

        private void sendRequest() throws Throwable {
            executors.add(this);
            ChannelFuture sendFuture = client.sendMessage(requestIdToWaitFor, requestData);
            sendFuture.sync();
        }

        /*
            The attachment request response as a 5 type ack response or
            6 type attachment response
         */
        public MessageData executeAndGetAttachmentQueryResult() throws Throwable {
            return getResult();
        }

        private void setAttachmentResult(String ownerTable, String attachmentId, MessageData data) {
            try {
                AttachmentResultHolder attachmentResultHolder =
                        new AttachmentResultHolderImpl(Collections.singletonList(requestIdToWaitFor),
                                ownerTable,
                                attachmentId);
                AttachmentResponseAckResultHolder resultHolder =
                        new AttachmentResponseAckResultHolderImpl(AckStatus.OK, attachmentResultHolder);
                MessageData7AttachmentResponseAckImpl responseAck =
                        new MessageData7AttachmentResponseAckImpl(AckStatus.OK, resultHolder, null);
                client.sendAttachmentResponseAck7(requestIdToWaitFor, responseAck);
                setResult(data);
            } catch (Throwable ex) {
                setCause(ex);
            }
        }

        private void messageReceived(MessageHeader header, MessageData data) {
            MessageHeaderBase headerBase = header.asBaseMessageHeader();
            if (data.isAttachmentResponseMessageData6()) {
                MessageData6AttachmentResponse attachmentResponse =
                        data.asAttachmentResponseMessageData6();
                /*
                    By type 6 the result request ids maybe contains the responses!
                 */
                if (attachmentResponse.getResult().getRequestIds().contains(requestIdToWaitFor)) {
                    setAttachmentResult(attachmentResponse.getResult().getOwnerTable(),
                            attachmentResponse.getResult().getAttachmentId(),
                            data);
                }
            } else if (requestIdToWaitFor.equals(headerBase.getMessageId())) {
                /*
                    By type 5 message, the message contains the request id
                 */
                if (data.isAttachmentRequestAckMessageData5()) {
                    MessageData5AttachmentRequestAck attachmentRequestAck =
                            data.asAttachmentRequestAckMessageData5();
                    if (!AckStatus.OK.equals(attachmentRequestAck.getGlobalStatus())) {
                        setResult(data);
                    } else if (null != attachmentRequestAck.getData().getResult().getAttachment()) {
                        setAttachmentResult(attachmentRequestAck.getData().getResult().getOwnerTable(),
                                attachmentRequestAck.getData().getResult().getAttachmentId(),
                                data);
                    }
                } else {
                    setResult(data);
                }
            }
        }

        private void setCause(Throwable cause) {
            synchronized (executorLock) {
                this.cause = cause;
                executorLock.notifyAll();
            }
        }

        private void setResult(MessageData data) {
            synchronized (executorLock) {
                result = data;
                executorLock.notifyAll();
            }
        }

        private void disconnected() {
            synchronized (executorLock) {
                cause = new SQLException("Connection lost with server");
                executorLock.notifyAll();
            }
        }
    }

    public GdsConnection(GdsClientURI clientURI) throws Throwable {
        AsyncGDSClient.AsyncGDSClientBuilder builder = AsyncGDSClient.getBuilder();
        builder
                .withUserName(clientURI.userName)
                .withUserPassword("".equals(clientURI.password) ? null : clientURI.password)
                .withLogger(LOG)
                .withServeOnTheSameConnection(clientURI.serveOnTheSameConnection)
                .withListener(new GDSMessageListener() {

                    public void onMessageReceived(MessageHeader header, MessageData data) {
                        for (OneTimeSyncTransactionExecutor executor : executors) {
                            executor.messageReceived(header, data);
                        }
                    }

                    @Override
                    public void onConnectionSuccess(Channel ch, MessageHeaderBase header, MessageData1ConnectionAck response) {
                        synchronized (lock) {
                            connected = true;
                            inited = true;
                            lock.notifyAll();
                        }
                    }

                    @Override
                    public void onConnectionFailure(Channel channel, Either<Throwable, Pair<MessageHeaderBase, MessageData1ConnectionAck>> reason) {
                        synchronized (lock) {
                            connected = false;
                            if (null != reason && reason.isLeftSet() && null != reason.getLeft()) {
                                GdsConnection.this.reason = reason.getLeft().getMessage();
                            } else if (null != reason && reason.isRightSet() && null != reason.getRight() && null != reason.getRight().getSecond()) {
                                GdsConnection.this.reason = reason.getRight().getSecond().getGlobalException();
                            }
                            inited = true;
                            lock.notifyAll();
                            try {
                                GdsConnection.this.close();
                            } catch (SQLException ignored) {
                            }
                        }
                        for (OneTimeSyncTransactionExecutor executor : executors) {
                            executor.disconnected();
                        }
                    }

                    @Override
                    public void onDisconnect(Channel channel) {
                        synchronized (lock) {
                            connected = false;
                            inited = true;
                            lock.notifyAll();
                            try {
                                GdsConnection.this.close();
                            } catch (SQLException ignored) {
                            }
                        }
                        for (OneTimeSyncTransactionExecutor executor : executors) {
                            executor.disconnected();
                        }
                    }

                    @Override
                    public void onEventAck3(MessageHeaderBase header, MessageData3EventAck response) {
                        onMessageReceived(header, response);
                    }

                    @Override
                    public void onAttachmentRequest4(MessageHeaderBase header, MessageData4AttachmentRequest request) {
                        onMessageReceived(header, request);
                    }

                    @Override
                    public void onAttachmentRequestAck5(MessageHeaderBase header, MessageData5AttachmentRequestAck requestAck) {
                        onMessageReceived(header, requestAck);
                    }

                    @Override
                    public void onAttachmentResponse6(MessageHeaderBase header, MessageData6AttachmentResponse response) {
                        onMessageReceived(header, response);
                    }

                    @Override
                    public void onAttachmentResponseAck7(MessageHeaderBase header, MessageData7AttachmentResponseAck responseAck) {
                        onMessageReceived(header, responseAck);
                    }

                    @Override
                    public void onEventDocument8(MessageHeaderBase header, MessageData8EventDocument eventDocument) {
                        onMessageReceived(header, eventDocument);
                    }

                    @Override
                    public void onEventDocumentAck9(MessageHeaderBase header, MessageData9EventDocumentAck eventDocumentAck) {
                        onMessageReceived(header, eventDocumentAck);
                    }

                    @Override
                    public void onQueryRequestAck11(MessageHeaderBase header, MessageData11QueryRequestAck response) {
                        onMessageReceived(header, response);
                    }
                });
        if (clientURI.sslEnabled) {
            builder
                    .withURI("wss://" + clientURI.host + "/" + clientURI.gateUrl)
                    .withTLS(clientURI.keyStorePath, clientURI.keyStorePassword);
        } else {
            builder
                    .withURI("ws://" + clientURI.host + "/" + clientURI.gateUrl);
        }
        client = builder.build();
    }

    public OneTimeSyncTransactionExecutor getNewExecutor(MessageData data, String queryId, long timeoutMillis) throws Throwable {
        return new OneTimeSyncTransactionExecutor(data, queryId, timeoutMillis);
    }

    /*
        Connect and send login to GDS
     */
    public void initConnection() throws Throwable {
        synchronized (lock) {
            if (closed) {
                client.close();
                throw new GdsException("Connection failed, already closed");
            }
            client.connect();
            initialized = true;
            if (!inited) {
                lock.wait();
            }
            if (!connected) {
                if (null != reason) {
                    throw new GdsException("Connection failed: " + reason);
                } else {
                    throw new GdsException("Connection failed");
                }
            }
        }
    }

    public void close() throws SQLException {
        synchronized (lock) {
            closed = true;
            client.close();
            if (!initialized) {
                return;
            }
            if (connected) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    throw new SQLException(e);
                }
            }
        }
    }

    public boolean isDisconnected() {
        return !connected;
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isInited() {
        return inited;
    }

    public String getReason() {
        return reason;
    }

    public AsyncGDSClient getClient() {
        return client;
    }
}
