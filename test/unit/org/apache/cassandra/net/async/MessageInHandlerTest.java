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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

@RunWith(Parameterized.class)
public class MessageInHandlerTest
{
    private static final int MSG_ID = 42;
    private static InetAddressAndPort addr;

    private final int messagingVersion;
    private final OutboundConnectionIdentifier connectionId;
    private final boolean handlesLargeMessages;

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.73.101"));
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    public MessageInHandlerTest(int messagingVersion, boolean handlesLargeMessages)
    {
        this.messagingVersion = messagingVersion;
        this.handlesLargeMessages = handlesLargeMessages;

        connectionId = handlesLargeMessages
                       ? OutboundConnectionIdentifier.large(addr, addr)
                       : OutboundConnectionIdentifier.small(addr, addr);
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
            { MessagingService.VERSION_30, false },
            { MessagingService.VERSION_30, true },
            { MessagingService.VERSION_40, false },
            { MessagingService.VERSION_40, true },
        });
    }

        private MessageInHandler getHandler(InetAddressAndPort addr, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        return new MessageInHandler(addr, MessageIn.getProcessor(addr, messagingVersion, messageConsumer), handlesLargeMessages);
    }

    @Test
    public void channelRead_HappyPath_NoParameters() throws Exception
    {
        MessageInWrapper result = channelRead_HappyPath(Collections.emptyMap());
        Assert.assertTrue(result.messageIn.parameters.isEmpty());
    }

    @Test
    public void channelRead_HappyPath_WithParameters() throws Exception
    {
        UUID uuid = UUIDGen.getTimeUUID();
        Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);
        parameters.put(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);
        parameters.put(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
        parameters.put(ParameterType.TRACE_SESSION, uuid);
        MessageInWrapper result = channelRead_HappyPath(parameters);
        Assert.assertEquals(3, result.messageIn.parameters.size());
        Assert.assertTrue(result.messageIn.isFailureResponse());
        Assert.assertEquals(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, result.messageIn.getFailureReason());
        Assert.assertEquals(uuid, result.messageIn.parameters.get(ParameterType.TRACE_SESSION));
    }

    private MessageInWrapper channelRead_HappyPath(Map<ParameterType, Object> parameters) throws Exception
    {
        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        for (Map.Entry<ParameterType, Object> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());
        serialize(msgOut);

        MessageInWrapper wrapper = new MessageInWrapper();
        MessageInHandler handler = getHandler(addr, wrapper.messageConsumer);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.writeInbound(buf);

        // need to wait until async tasks are complete, as large messages spin up a background thread
        Assert.assertTrue(wrapper.latch.await(5, TimeUnit.SECONDS));
        Assert.assertNotNull(wrapper.messageIn);
        Assert.assertEquals(MSG_ID, wrapper.id);
        Assert.assertEquals(msgOut.from, wrapper.messageIn.from);
        Assert.assertEquals(msgOut.verb, wrapper.messageIn.verb);

        return wrapper;
    }

    private void serialize(MessageOut msgOut) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        int timestamp = (int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime());
        msgOut.serialize(new ByteBufDataOutputPlus(buf), messagingVersion, connectionId, MSG_ID, timestamp);
    }

    @Test
    public void exceptionHandled()
    {
        MessageInHandler handler = getHandler(addr, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        handler.exceptionCaught(channel.pipeline().firstContext(), new EOFException());
        Assert.assertFalse(channel.isOpen());
    }

    private static class MessageInWrapper
    {
        final CountDownLatch latch = new CountDownLatch(1);
        MessageIn messageIn;
        int id;

        final BiConsumer<MessageIn, Integer> messageConsumer = (messageIn, integer) ->
        {
            this.messageIn = messageIn;
            this.id = integer;
            latch.countDown();
        };
    }
}
