/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Represents one connection to a peer, and handles the state transistions on the connection and the netty {@link Channel}
 * The underlying socket is not opened until explicitly requested (by sending a message).
 *
 * The basic setup for the channel is like this: a message is requested to be sent via {@link #sendMessage(MessageOut, int)}.
 * If the channel is not established, then we need to create it (obviously). To prevent multiple threads from creating
 * independent connections, they attempt to update the {@link #state}; one thread will win the race and create the connection.
 * Upon sucessfully setting up the connection/channel, the {@link #state} will be updated again (to {@link State#READY},
 * which indicates to other threads that the channel is ready for business and can be written to.
 *
 * // TODO:JEB probably expand me
 */
public class OutboundMessagingConnection
{
    static final Logger logger = LoggerFactory.getLogger(OutboundMessagingConnection.class);

    private static final String INTRADC_TCP_NODELAY_PROPERTY = Config.PROPERTY_PREFIX + "otc_intradc_tcp_nodelay";

    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /**
     * Number of milliseconds between connection createRetry attempts.
     */
    private static final int OPEN_RETRY_DELAY_MS = 200;

    /**
     * A minimum number of milliseconds to wait for a connection (TCP socket connect + handshake)
     */
    private static final int MINIMUM_CONNECT_TIMEOUT_MS = 2000;

    /**
     * Some (artificial) max number of time to attempt to connect at a given time.
     */
    private static final int MAX_RECONNECT_ATTEMPTS = 10;

    // TODO:JEB document me!!
    static final int STATE_CONNECTION_CREATE_IDLE = 0;
    static final int STATE_CONNECTION_CREATE_RUNNING = 1;
    static final int STATE_CONNECTION_CLOSED = 2;

    // TODO:JEB document me!!
    static final int QUEUE_CONSUMER_STATE_IDLE = 0;
    static final int QUEUE_CONSUMER_STATE_ENQUEUED = 1;
    static final int QUEUE_CONSUMER_STATE_RUNNING = 2;

    private final IInternodeAuthenticator authenticator;

    /**
     * Backlog to hold messages passed by producer threads while a consumer task  sned them one by one to
     * the Netty {@link Channel}/{@link #channelWriter}.
     */
    private final Queue<QueuedMessage> backlog;

    /**
     * The number of items in the {@link #backlog}. Needed as we do not have a constant-time lookup for size
     * in the {@link #backlog}.
     */
    private final AtomicInteger backlogSize;

    private final AtomicLong droppedMessageCount;
    private final AtomicLong completedMessageCount;

    private volatile OutboundConnectionIdentifier connectionId;

    private final ServerEncryptionOptions encryptionOptions;

    /**
     * A future for retrying connections.
     * // TODO:JEB doc me
     */
    private ScheduledFuture<?> connectionRetryFuture;

    /**
     * A future for notifying when the timeout for creating the connection and negotiating the handshake has elapsed.
     * It will be cancelled when the channel is established correctly. This future executes in the netty event loop.
     */
    private ScheduledFuture<?> connectionTimeoutFuture;

    private final CoalescingStrategy coalescingStrategy;

    // TODO:JEB document me
    private final EventLoop eventLoop;

    /**
     * A running count of the number of times we've tried to create a connection. Is reset upon a successful connection creation.
     *
     * As it is only referenced on the event loop, this field does not need to be volatile.
     */
    private int connectAttemptCount;

    /**
     * The netty channel, once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private volatile ChannelWriter channelWriter;

    /**
     * the target protocol version to communicate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    // TODO:JEB doc me -- also, FIX ME !!!!  also, rename me?
    // to protect against concurrent (re-)connect attempts
    private final AtomicInteger state;

    // TODO:JEB doc me
    private volatile int queueConsumerState;
    private static final AtomicIntegerFieldUpdater<OutboundMessagingConnection> queueConsumerStateUpdater = AtomicIntegerFieldUpdater.newUpdater(OutboundMessagingConnection.class, "queueConsumerState");

    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                CoalescingStrategy coalescingStrategy,
                                IInternodeAuthenticator authenticator)
    {
        this.connectionId = connectionId;
        this.encryptionOptions = encryptionOptions;
        this.authenticator = authenticator;
        backlog = new ConcurrentLinkedQueue<>();
        backlogSize = new AtomicInteger(0);
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        state = new AtomicInteger(STATE_CONNECTION_CREATE_IDLE);
        this.coalescingStrategy = coalescingStrategy;
        queueConsumerState = QUEUE_CONSUMER_STATE_IDLE;

        eventLoop = NettyFactory.instance.outboundGroup.next();

        // We want to use the most precise protocol version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // only connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us, and in both cases that will set the version).
        // In that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(connectionId.remote());
    }

    private String loggingTag()
    {
        ChannelWriter writer = channelWriter;
        String channelId = writer != null ? writer.channel.id().asShortText() : "[no-channel]";
        return connectionId.remote() + "-" + connectionId.type() + "-" + channelId;
    }

    boolean sendMessage(MessageOut msg, int id)
    {
        return sendMessage(new QueuedMessage(msg, id));
    }

    boolean sendMessage(QueuedMessage queuedMessage)
    {
        if (isClosed())
            return false;

        backlogSize.incrementAndGet();
        backlog.add(queuedMessage);

        if (queueConsumerStateUpdater.compareAndSet(this, QUEUE_CONSUMER_STATE_IDLE, QUEUE_CONSUMER_STATE_ENQUEUED))
            eventLoop.submit(this::maybeStartDequeuing);

        // TODO:JEB correct this, mostly for the backlog (if I'm gonna have a fixed length queue or something...)
        return true;
    }

    // TODO:JEB doc me, but this should only execute on the event loop
    boolean maybeStartDequeuing()
    {
        if (isClosed())
            return false;

        if (backlog.isEmpty())
        {
            queueConsumerStateUpdater.set(this, QUEUE_CONSUMER_STATE_IDLE);
            return false;
        }

        if (channelWriter == null || channelWriter.isClosed())
        {
            // note: do not change queueConsumerState here else a producer thread could inadvertently schedule another
            // task, which would attempt to connect, as well, and so on
            reconnect();
            return false;
        }

        if (!queueConsumerStateUpdater.compareAndSet(this, QUEUE_CONSUMER_STATE_ENQUEUED, QUEUE_CONSUMER_STATE_RUNNING))
            return false;

        return dequeueMessage();
    }

    // TODO:JEB doc me, but assumes a) we're on the event loop, b) channel/channelWriter are set up, c) state == QUEUE_CONSUMER_STATE_RUNNING
    //, and d) not closed
    private boolean dequeueMessage()
    {
        logger.debug("{} JEB::OMC::dequeueMessage HEAD state = {}, queueState = {}", loggingTag(), state, queueConsumerState);
        if (!channelWriter.channel.isWritable())
            return false;

        boolean messageSent = false;
        while (!messageSent)
        {
            QueuedMessage next = backlog.poll();
            if (next == null)
                break;

            backlogSize.decrementAndGet();

            if (!next.isTimedOut())
            {
                logger.debug("{} JEB::OMC::dequeueMessage - channel {} sending message {}", loggingTag(), connectionId.type(), next);
                channelWriter.write(next).addListener(future -> handleMessageResult(next, future));
                messageSent = true;
            }
            else
            {
                droppedMessageCount.incrementAndGet();
            }
        }

        // note: we have to set the state back to IDLE if we did not send a message to channelWriter,
        // because there's no other component that will do it. handleMessageResult() sets the state correctly
        // when a message *is* written to the channleWriter
        // TODO:JEB i might need Nitesh's check here
        if (!messageSent)
            queueConsumerStateUpdater.set(this, QUEUE_CONSUMER_STATE_IDLE);

        return messageSent;
    }

    /**
     * Initiate all the actions required to establish a working, valid connection. This includes
     * opening the socket, negotiating the internode messaging handshake, and setting up the working
     * Netty {@link Channel}. However, this method will not block for all those actions: it will only
     * kick off the connection attempt as everything is asynchronous.
     * <p>
     * Note: this should only be invoked on the event loop.
     */
    boolean connect(boolean cancelTimeout)
    {
        logger.debug("{} JEB::OMC::connect HEAD - state = {}, queuState = {}", loggingTag(), state, queueConsumerState);

        // clean up any lingering connection attempts
        cleanupConnectArtifacts(cancelTimeout);

        if (isClosed())
            return false;

        logger.debug("{} connection attempt {}", loggingTag(), connectAttemptCount);

        InetAddressAndPort remote = connectionId.remote();
        if (!authenticator.authenticate(remote.address, remote.port))
        {
            logger.warn("{} Internode auth failed", loggingTag());
            //Remove the connection pool and other thread so messages aren't queued
            MessagingService.instance().destroyConnectionPool(remote);

            // don't update the state field as destroyConnectionPool() *should* call OMC.close()
            // on all the connections in the OMP for the remoteAddress
            return false;
        }

        boolean compress = shouldCompressConnection(connectionId.local(), connectionId.remote());
        maybeUpdateConnectionId();
        Bootstrap bootstrap = buildBootstrap(compress);
        ChannelFuture connectFuture = bootstrap.connect();
        connectFuture.addListener(this::connectCallback);

        long timeout = Math.max(MINIMUM_CONNECT_TIMEOUT_MS, DatabaseDescriptor.getRpcTimeout());

        if (connectionTimeoutFuture == null || connectionTimeoutFuture.isDone())
            connectionTimeoutFuture = eventLoop.schedule(() -> connectionTimeout(connectFuture), timeout, TimeUnit.MILLISECONDS);
        return true;
    }

    @VisibleForTesting
    static boolean shouldCompressConnection(InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !isLocalDC(localHost, remoteHost));
    }

    /**
     * After a bounce we won't necessarily know the peer's version, so we assume the peer is at least 4.0
     * and thus using a single port for secure and non-secure communication. However, during a rolling upgrade from
     * 3.0.x/3.x to 4.0, the not-yet upgraded peer is still listening on separate ports, but we don't know the peer's
     * version until we can successfully connect. Fortunately, the peer can connect to this node, at which point
     * we'll grab it's version. We then use that knowledge to use the {@link Config#ssl_storage_port} to connect on,
     * and to do that we need to update some member fields in this instance.
     *
     * Note: can be removed at 5.0
     */
    void maybeUpdateConnectionId()
    {
        if (encryptionOptions != null)
        {
            int version = MessagingService.instance().getVersion(connectionId.remote());
            if (version < targetVersion)
            {
                targetVersion = version;
                int port = MessagingService.instance().portFor(connectionId.remote());
                connectionId = connectionId.withNewConnectionPort(port);
                logger.debug("{} changing connectionId to {}, with a different port for secure communication, because peer version is {}",
                             loggingTag(), connectionId, version);
            }
        }
    }

    private Bootstrap buildBootstrap(boolean compress)
    {
        boolean tcpNoDelay = isLocalDC(connectionId.local(), connectionId.remote()) ? INTRADC_TCP_NODELAY : DatabaseDescriptor.getInterDCTcpNoDelay();
        int sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeSendBufferSize()
                             : OutboundConnectionParams.DEFAULT_SEND_BUFFER_SIZE;

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .callback(this::finishHandshake)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(Mode.MESSAGING)
                                                                  .compress(compress)
                                                                  .coalescingStrategy(coalescingStrategy)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .tcpNoDelay(tcpNoDelay)
                                                                  .protocolVersion(targetVersion)
                                                                  .eventLoop(eventLoop)
                                                                  .pendingMessageCountSupplier(backlogSize::get)
                                                                  .build();

        return NettyFactory.instance.createOutboundBootstrap(params);
    }

    static boolean isLocalDC(InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(remoteHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(localHost);
        return remoteDC != null && remoteDC.equals(localDC);
    }

    /**
     * Handles the callback of the TCP connection attempt (not including the handshake negotiation!), and really all
     * we're handling here is the TCP connection failures. On failure, we close the channel (which should disconnect
     * the socket, if connected). If there was an {@link IOException} while trying to connect, the connection will be
     * retried after a short delay.
     * <p>
     * This method does not alter the {@link #state} as it's only evaluating the TCP connect, not TCP connect and handshake.
     * Thus, {@link #finishHandshake(HandshakeResult)} will handle any necessary state updates.
     * <p>
     * Note: this method is called from the event loop.
     *
     * @return true iff the TCP connection was established and the {@link #state} is not {@link State#CLOSED}; else false.
     */
    @VisibleForTesting
    boolean connectCallback(Future<? super Void> future)
    {
        ChannelFuture channelFuture = (ChannelFuture)future;

        // make sure this instance is not (terminally) closed
        if (isClosed())
        {
            // we don't have a ChannelWriter just yet, so we have to go directly to the future's channel to close it
            channelFuture.channel().close();
            return false;
        }

        assert state.get() == STATE_CONNECTION_CREATE_RUNNING;

        // this is the success state
        final Throwable cause = future.cause();
        if (cause == null)
            return true;

        if (cause instanceof IOException)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} unable to connect on attempt {}", loggingTag(), connectAttemptCount, cause);

            if (connectAttemptCount < MAX_RECONNECT_ATTEMPTS)
            {
                connectAttemptCount++;
                connectionRetryFuture = eventLoop.schedule(() -> connect(false), OPEN_RETRY_DELAY_MS * connectAttemptCount, TimeUnit.MILLISECONDS);
            }
            else
            {
                setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
            }
        }
        else
        {
            JVMStabilityInspector.inspectThrowable(cause);
            logger.error("{} non-IO error attempting to connect", loggingTag(), cause);
            cleanupConnectArtifacts(true);
            setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
            queueConsumerStateUpdater.set(this, QUEUE_CONSUMER_STATE_IDLE);
        }
        return false;
    }

    /**
     * A callback for handling timeouts when creating a connection/negotiating the handshake.
     *
     * Note: this method is invoked from the netty event loop.
     *
     * @return true if there was a timeout on the connect/handshake; else false.
     */
    boolean connectionTimeout(ChannelFuture channelFuture)
    {
        logger.debug("{} JEB::OMC::connectionTimeout triggered", loggingTag());
        cleanupConnectArtifacts(true);
        if (isClosed())
            return false;

        if (logger.isDebugEnabled())
            logger.debug("{} timed out while trying to connect", loggingTag());

        setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
        queueConsumerStateUpdater.set(this, QUEUE_CONSUMER_STATE_IDLE);
        channelFuture.channel().close();
        purgeBacklog();
        return true;
    }

    /**
     * Process the results of the handshake negotiation. This should only be invoked after {@link #connectCallback(Future)}
     * has successfully be invoked (as we need the TCP connection to be properly established before any
     * application messages can be sent).
     * <p>
     * Note: this method will be invoked from the netty event loop.
     */
    void finishHandshake(HandshakeResult result)
    {
        // clean up the connector instances before changing the state
        cleanupConnectArtifacts(true);

        if (result.negotiatedMessagingVersion != HandshakeResult.UNKNOWN_PROTOCOL_VERSION)
        {
            targetVersion = result.negotiatedMessagingVersion;
            MessagingService.instance().setVersion(connectionId.remote(), targetVersion);
        }

        switch (result.outcome)
        {
            case SUCCESS:
                assert result.channelWriter != null;

                if (isClosed() || result.channelWriter.isClosed())
                {
                    result.channelWriter.close();
                    setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
                    purgeBacklog();
                    break;
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("{} successfully connected, compress = {}, coalescing = {}", loggingTag(),
                                 shouldCompressConnection(connectionId.local(), connectionId.remote()),
                                 coalescingStrategy != null ? coalescingStrategy : CoalescingStrategies.Strategy.DISABLED);
                }
                channelWriter = result.channelWriter;
                setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
                maybeStartDequeuing();
                break;
            case DISCONNECT:
                setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
                reconnect();
                break;
            case NEGOTIATION_FAILURE:
                setStateIfNotClosed(state, STATE_CONNECTION_CREATE_IDLE);
                purgeBacklog();
                break;
            default:
                throw new IllegalArgumentException("unhandled result type: " + result.outcome);
        }
    }

    /**
     * Cleanup member fields used when connecting to a peer.
     *
     * Note: This will execute on the event loop
     */
    private void cleanupConnectArtifacts(boolean resetConnectTimeout)
    {
        if (resetConnectTimeout)
        {
            if (connectionTimeoutFuture != null)
            {
                connectionTimeoutFuture.cancel(false);
                connectionTimeoutFuture = null;
            }

            if (resetConnectTimeout)
                connectAttemptCount = 0;
        }

        if (connectionRetryFuture != null)
        {
            connectionRetryFuture.cancel(false);
            connectionRetryFuture = null;
        }
    }

    @VisibleForTesting
    static boolean setStateIfNotClosed(AtomicInteger state, int nextState)
    {
        int currentState = state.get();
        if (currentState == STATE_CONNECTION_CLOSED)
            return false;
        return state.compareAndSet(currentState, nextState);
    }

    int getTargetVersion()
    {
        return targetVersion;
    }

    /**
     * Handles the result of each message sent.
     *
     * // TODO:JEB this will not be invoked on the event loop in the case of large messages (CASSANDRA-13630)
     * Note: this function is expected to be invoked on the netty event loop. Also, do not retain any state from
     * the input {@code messageResult}.
     */
    void handleMessageResult(QueuedMessage message, Future<? super Void> future)
    {
        completedMessageCount.incrementAndGet();

        // checking the cause() is an optimized way to tell if the operation was successful (as the cause will be null)
        // Note that ExpiredException is just a marker for timeout-ed message we're dropping, but as we already
        // incremented the dropped message count in MessageOutHandler, we have nothing to do.
        Throwable cause = future.cause();
        if (cause == null)
        {
            if (!dequeueMessage())
            {
                queueConsumerStateUpdater.set(this, QUEUE_CONSUMER_STATE_IDLE);
                // TODO:JEB doc me
                 if (!backlog.isEmpty() && queueConsumerStateUpdater.compareAndSet(this, QUEUE_CONSUMER_STATE_IDLE, QUEUE_CONSUMER_STATE_ENQUEUED))
                     dequeueMessage();
            }
            return;
        }

        JVMStabilityInspector.inspectThrowable(cause);
        queueConsumerStateUpdater.set(this, QUEUE_CONSUMER_STATE_IDLE);

        // TODO::JEB not a fan of this extra flag
        boolean reconnected = false;
        if (cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            if (channelWriter.shouldPurgeBacklog())
                purgeBacklog();

            // This writer needs to be closed and we need to trigger a reconnection. We really only want to do that
            // once for this channel however (and again, no race because we're on the netty event loop).
            if (!channelWriter.isClosed())
            {
                reconnected = true;
                reconnect();
                channelWriter.close();
            }

            // maybe reenqueue the victim message
            if (message != null && message.shouldRetry())
                sendMessage(message.createRetry());
        }
        else if (future.isCancelled())
        {
            // Someone cancelled the future, which we assume meant it doesn't want the message to be sent if it hasn't
            // yet. Just ignore.
        }
        else
        {
            // Non IO exceptions are likely a programming error so let's not silence them
            logger.error("{} Unexpected error writing", loggingTag(), cause);
        }

        if (!reconnected && !backlog.isEmpty() && queueConsumerStateUpdater.compareAndSet(this, QUEUE_CONSUMER_STATE_IDLE, QUEUE_CONSUMER_STATE_ENQUEUED))
            dequeueMessage();
    }

    /**
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address if there
     * was a previous connection, and new incoming messages as well as existing {@link #backlog} messages will be sent there.
     * Any outstanding messages in the existing channel will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     *
     * Note: this will not be invoked on the event loop.
     */
    void reconnectWithNewIp(InetAddressAndPort newAddr)
    {
        // if we're closed, ignore the request
        if (isClosed())
            return;

        // capture a reference to the current channel, in case it gets swapped out before we can call close() on it
        ChannelWriter currentChannel = channelWriter;
        connectionId = connectionId.withNewConnectionAddress(newAddr);
        reconnect();

        // lastly, push through anything remaining in the existing channel.
        if (currentChannel != null)
            currentChannel.softClose();
    }

    /**
     * Schedule {@link #connect(boolean)} to attempt to reconnect.
     *
     * @return A future is this thread won a race to kick off the connect process; else, null.
     */
    Future<?> reconnect()
    {
        if (state.compareAndSet(STATE_CONNECTION_CREATE_IDLE, STATE_CONNECTION_CREATE_RUNNING))
            return eventLoop.submit(() -> connect(true));

        // TODO:JEB fix this
        return null;
    }

    void purgeBacklog()
    {
        logger.debug("{} JEB::OMC::purgeBacklog", loggingTag(), new Exception("JEB dump stack on purge"));
        int droppedCount = backlogSize.getAndSet(0);
        droppedMessageCount.addAndGet(droppedCount);
        backlog.clear();
    }

    /**
     * Permanently close this instance.
     *
     * Note: should not be invoked on the event loop.
     */
    public void close(boolean softClose)
    {
        if (isClosed())
            return;

        cleanupConnectArtifacts(true);

        // drain the backlog
        if (channelWriter != null)
        {
            if (softClose)
            {
                channelWriter.softClose();
            }
            else
            {
                purgeBacklog();
                channelWriter.close();
            }
        }

        if (!softClose)
            state.set(STATE_CONNECTION_CLOSED);
    }

    @Override
    public String toString()
    {
        return connectionId.toString();
    }

    public Integer getPendingMessages()
    {
        return backlogSize.get();
    }

    public Long getCompletedMessages()
    {
        return completedMessageCount.get();
    }

    public Long getDroppedMessages()
    {
        return droppedMessageCount.get();
    }

    /*
        methods specific to testing follow
     */

    @VisibleForTesting
    void addToBacklog(QueuedMessage msg)
    {
        backlogSize.incrementAndGet();
        backlog.add(msg);
    }

    @VisibleForTesting
    void setChannelWriter(ChannelWriter channelWriter)
    {
        this.channelWriter = channelWriter;
    }

    @VisibleForTesting
    ChannelWriter getChannelWriter()
    {
        return channelWriter;
    }

    @VisibleForTesting
    void setTargetVersion(int targetVersion)
    {
        this.targetVersion = targetVersion;
    }

    @VisibleForTesting
    OutboundConnectionIdentifier getConnectionId()
    {
        return connectionId;
    }

    @VisibleForTesting
    void setConnectionTimeoutFuture(ScheduledFuture<?> connectionTimeoutFuture)
    {
        this.connectionTimeoutFuture = connectionTimeoutFuture;
    }

    @VisibleForTesting
    ScheduledFuture<?> getConnectionTimeoutFuture()
    {
        return connectionTimeoutFuture;
    }

    @VisibleForTesting
    void setQueueConsumerState(int state)
    {
        queueConsumerState = state;
    }

    public boolean isConnected()
    {
        return channelWriter != null;
    }

    public boolean isClosed()
    {
        return state.get() == STATE_CONNECTION_CLOSED;
    }

    void setState(int nextState)
    {
        state.set(nextState);
    }

    int getState()
    {
        return state.get();
    }

    /**
     * Use only in tests as {@link #backlog} is not guaranteed to have constant-time performance.
     * Use {@link #getPendingMessages()} instead.
     */
    int getBacklogSize()
    {
        return backlog.size();
    }
}