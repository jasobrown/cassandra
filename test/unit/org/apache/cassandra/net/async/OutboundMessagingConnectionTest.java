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
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLHandshakeException;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceTest;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.STATE_CONNECTION_CLOSED;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.STATE_CONNECTION_CREATE_IDLE;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.STATE_CONNECTION_CREATE_RUNNING;

public class OutboundMessagingConnectionTest
{
    private static final InetAddressAndPort LOCAL_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 9998);
    private static final InetAddressAndPort REMOTE_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 9999);
    private static final InetAddressAndPort RECONNECT_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.3"), 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private OutboundConnectionIdentifier connectionId;
    private NonSendingOutboundMessagingConnection omc;
    private ChannelWriter channelWriter;
    private EmbeddedChannel channel;

    private IEndpointSnitch snitch;
    private ServerEncryptionOptions encryptionOptions;
    private MessageResult messageResult;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        connectionId = OutboundConnectionIdentifier.small(LOCAL_ADDR, REMOTE_ADDR);
        omc = new NonSendingOutboundMessagingConnection(connectionId, null, null);
        channel = new EmbeddedChannel();
        OutboundConnectionParams params = OutboundConnectionParams.builder().messageResultConsumer(omc::handleMessageResult).build();
        channelWriter = ChannelWriter.create(channel, params);
        omc.setChannelWriter(channelWriter);

        snitch = DatabaseDescriptor.getEndpointSnitch();
        encryptionOptions = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        messageResult = new MessageResult();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryptionOptions);
        channel.finishAndReleaseAll();
    }

    @Test
    public void sendMessage_CreatingChannel()
    {
        assertBacklogSizes(0);
        omc.setState(STATE_CONNECTION_CREATE_RUNNING);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(1);
    }

    private void assertBacklogSizes(int size)
    {
        Assert.assertEquals(size, omc.getPendingMessages().intValue());
        Assert.assertEquals(size, omc.getBacklogSize());
    }

    @Test
    public void sendMessage_HappyPath()
    {
        assertBacklogSizes(0);
        omc.setState(STATE_CONNECTION_CREATE_IDLE);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(0);
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void sendMessage_Closed()
    {
        assertBacklogSizes(0);
        omc.setState(STATE_CONNECTION_CLOSED);
        Assert.assertFalse(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(0);
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void shouldCompressConnection_None()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        Assert.assertFalse(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_All()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        Assert.assertTrue(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_SameDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR, "dc1");
        snitch.add(REMOTE_ADDR, "dc1");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertFalse(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    private static class TestSnitch extends AbstractEndpointSnitch
    {
        private final Map<InetAddressAndPort, String> nodeToDc = new HashMap<>();

        void add(InetAddressAndPort node, String dc)
        {
            nodeToDc.put(node, dc);
        }

        public String getRack(InetAddressAndPort endpoint)
        {
            return null;
        }

        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return nodeToDc.get(endpoint);
        }

        public int compareEndpoints(InetAddressAndPort target, InetAddressAndPort a1, InetAddressAndPort a2)
        {
            return 0;
        }
    }

    @Test
    public void shouldCompressConnection_DifferentDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR, "dc1");
        snitch.add(REMOTE_ADDR, "dc2");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void close_softClose()
    {
        close(true);
    }

    @Test
    public void close_hardClose()
    {
        close(false);
    }

    private void close(boolean softClose)
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.setChannelWriter(channelWriter);

        omc.close(softClose);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(STATE_CONNECTION_CLOSED, omc.getState());
        assertBacklogSizes(0);
        int sentMessages = channel.outboundMessages().size();

        if (softClose)
            Assert.assertTrue(count <= sentMessages);
        else
            Assert.assertEquals(0, sentMessages);
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
        Assert.assertTrue(channelWriter.isClosed());
    }

    @Test
    public void connect_IInternodeAuthFail()
    {
        IInternodeAuthenticator auth = new IInternodeAuthenticator()
        {
            public boolean authenticate(InetAddress remoteAddress, int remotePort)
            {
                return false;
            }

            public void validateConfiguration() throws ConfigurationException
            {

            }
        };

        MessageOut messageOut = new MessageOut(MessagingService.Verb.GOSSIP_DIGEST_ACK);
        OutboundMessagingPool pool = new OutboundMessagingPool(REMOTE_ADDR, LOCAL_ADDR, null,
                                                               new MessagingServiceTest.MockBackPressureStrategy(null).newState(REMOTE_ADDR), auth);
        OutboundMessagingConnection omc = pool.getConnection(messageOut);
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
        Assert.assertFalse(omc.connect());
    }

    @Test
    public void connect_ConnectionAlreadyStarted()
    {
        omc.setState(STATE_CONNECTION_CREATE_RUNNING);
        Assert.assertNull(omc.reconnect());
        Assert.assertSame(STATE_CONNECTION_CREATE_RUNNING, omc.getState());
    }

    @Test
    public void connect_ConnectionClosed()
    {
        omc.setState(STATE_CONNECTION_CLOSED);
        Assert.assertNull(omc.reconnect());
        Assert.assertEquals(STATE_CONNECTION_CLOSED, omc.getState());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        omc.setState(STATE_CONNECTION_CREATE_IDLE);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertFalse(omc.connectionTimeout(channelFuture));
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
    }

    @Test
    public void connectionTimeout_StateIsClosed()
    {
        omc.setState(STATE_CONNECTION_CLOSED);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertTrue(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(STATE_CONNECTION_CLOSED, omc.getState());
    }

    @Test
    public void connectionTimeout_AssumeConnectionTimedOut()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        omc.setState(STATE_CONNECTION_CREATE_RUNNING);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertTrue(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
        assertBacklogSizes(0);
    }

    @Test
    public void connectCallback_FutureIsSuccess()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        Assert.assertTrue(omc.connectCallback(promise));
    }

    @Test
    public void connectCallback_Closed()
    {
        ChannelPromise promise = channel.newPromise();
        omc.setState(STATE_CONNECTION_CLOSED);
        Assert.assertFalse(omc.connectCallback(promise));
    }

    @Test
    public void connectCallback_FailCauseIsSslHandshake()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new SSLHandshakeException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
    }

    @Test
    public void connectCallback_FailCauseIsNPE()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
    }

    @Test
    public void connectCallback_FailCauseIsIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
    }

    @Test
    public void connectCallback_FailedAndItsClosed()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        omc.setState(STATE_CONNECTION_CLOSED);
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertTrue(omc.isClosed());
    }

    @Test
    public void finishHandshake_GOOD()
    {
        HandshakeResult result = HandshakeResult.success(channelWriter, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.setChannelWriter(null);
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.finishHandshake(result);
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertEquals(channelWriter, omc.getChannelWriter());
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNull(omc.getConnectionTimeoutFuture());
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_GOOD_ButClosed()
    {
        HandshakeResult result = HandshakeResult.success(channelWriter, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.setChannelWriter(null);
        omc.setState(STATE_CONNECTION_CLOSED);
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.finishHandshake(result);
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertNull(omc.getChannelWriter());
        Assert.assertEquals(STATE_CONNECTION_CLOSED, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNull(omc.getConnectionTimeoutFuture());
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_DISCONNECT()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        HandshakeResult result = HandshakeResult.disconnect(MESSAGING_VERSION);
        omc.finishHandshake(result);
        Assert.assertNotNull(omc.getChannelWriter());
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        assertBacklogSizes(0);
    }

    @Test
    public void finishHandshake_CONNECT_FAILURE()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        HandshakeResult result = HandshakeResult.failed();
        omc.finishHandshake(result);
        Assert.assertEquals(STATE_CONNECTION_CREATE_IDLE, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        assertBacklogSizes(0);
    }

    @Test
    public void setStateIfNotClosed_AlreadyClosed()
    {
        AtomicInteger state = new AtomicInteger(STATE_CONNECTION_CLOSED);
        OutboundMessagingConnection.setStateIfNotClosed(state, STATE_CONNECTION_CREATE_RUNNING);
        Assert.assertEquals(STATE_CONNECTION_CLOSED, state.get());
    }

    @Test
    public void setStateIfNotClosed_NotClosed()
    {
        AtomicInteger state = new AtomicInteger(STATE_CONNECTION_CREATE_IDLE);
        OutboundMessagingConnection.setStateIfNotClosed(state, STATE_CONNECTION_CREATE_RUNNING);
        Assert.assertEquals(STATE_CONNECTION_CREATE_RUNNING, state.get());
    }

    @Test
    public void reconnectWithNewIp_HappyPath()
    {
        omc.setChannelWriter(channelWriter);
        omc.setState(STATE_CONNECTION_CREATE_RUNNING);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertFalse(omc.getConnectionId().equals(originalId));
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertNotSame(STATE_CONNECTION_CLOSED, omc.getState());
    }

    @Test
    public void reconnectWithNewIp_Closed()
    {
        omc.setState(STATE_CONNECTION_CLOSED);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertSame(omc.getConnectionId(), originalId);
        Assert.assertEquals(STATE_CONNECTION_CLOSED, omc.getState());
    }

    @Test
    public void reconnectWithNewIp_UnsedConnection()
    {
        omc.setState(STATE_CONNECTION_CREATE_RUNNING);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertNotSame(omc.getConnectionId(), originalId);
        Assert.assertSame(STATE_CONNECTION_CREATE_RUNNING, omc.getState());
    }

    @Test
    public void maybeUpdateConnectionId_NoEncryption()
    {
        OutboundConnectionIdentifier connectionId = omc.getConnectionId();
        int version = omc.getTargetVersion();
        omc.maybeUpdateConnectionId();
        Assert.assertEquals(connectionId, omc.getConnectionId());
        Assert.assertEquals(version, omc.getTargetVersion());
    }

    @Test
    public void maybeUpdateConnectionId_SameVersion()
    {
        ServerEncryptionOptions encryptionOptions = new ServerEncryptionOptions();
        omc = new NonSendingOutboundMessagingConnection(connectionId, encryptionOptions, null);
        OutboundConnectionIdentifier connectionId = omc.getConnectionId();
        int version = omc.getTargetVersion();
        omc.maybeUpdateConnectionId();
        Assert.assertEquals(connectionId, omc.getConnectionId());
        Assert.assertEquals(version, omc.getTargetVersion());
    }

    @Test
    public void maybeUpdateConnectionId_3_X_Version()
    {
        ServerEncryptionOptions encryptionOptions = new ServerEncryptionOptions();
        encryptionOptions.enabled = true;
        encryptionOptions.internode_encryption = ServerEncryptionOptions.InternodeEncryption.all;
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryptionOptions);
        omc = new NonSendingOutboundMessagingConnection(connectionId, encryptionOptions, null);
        int peerVersion = MessagingService.VERSION_30;
        MessagingService.instance().setVersion(connectionId.remote(), MessagingService.VERSION_30);

        OutboundConnectionIdentifier connectionId = omc.getConnectionId();
        omc.maybeUpdateConnectionId();
        Assert.assertNotEquals(connectionId, omc.getConnectionId());
        Assert.assertEquals(InetAddressAndPort.getByAddressOverrideDefaults(REMOTE_ADDR.address, DatabaseDescriptor.getSSLStoragePort()), omc.getConnectionId().remote());
        Assert.assertEquals(InetAddressAndPort.getByAddressOverrideDefaults(REMOTE_ADDR.address, DatabaseDescriptor.getSSLStoragePort()), omc.getConnectionId().connectionAddress());
        Assert.assertEquals(peerVersion, omc.getTargetVersion());
    }

    @Test
    public void handleMessageResult_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        messageResult.setAll(channelWriter, new QueuedMessage(new MessageOut<>(ECHO), 1), promise);
        omc.handleMessageResult(messageResult);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
    }

    @Test
    public void handleMessageResult_ExpiredException_DoNotRetryMsg()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException());

        messageResult.setAll(channelWriter, new QueuedMessage(new MessageOut<>(ECHO), 1), promise);
        omc.handleMessageResult(messageResult);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(1, omc.getDroppedMessages().longValue());
        Assert.assertFalse(omc.sendMessageInvoked);
    }

    @Test
    public void handleMessageResult_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        messageResult.setAll(channelWriter, new QueuedMessage(new MessageOut<>(ECHO), 1), promise);
        omc.handleMessageResult(messageResult);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertFalse(omc.sendMessageInvoked);
    }

    @Test
    public void handleMessageResult_IOException_ChannelNotClosed_RetryMsg()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        Assert.assertTrue(channel.isActive());

        messageResult.setAll(channelWriter, new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true), promise);
        omc.handleMessageResult(messageResult);

        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertTrue(omc.sendMessageInvoked);
    }

    @Test
    public void handleMessageResult_Cancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        Assert.assertTrue(channel.isActive());
        QueuedMessage message = new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true);
        messageResult.setAll(channelWriter, message, promise);
        omc.handleMessageResult(messageResult);

        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertFalse(omc.sendMessageInvoked);
    }
}
