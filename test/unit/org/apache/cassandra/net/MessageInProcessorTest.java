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

package org.apache.cassandra.net;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.ByteBufDataInputPlus;

@RunWith(Parameterized.class)
public class MessageInProcessorTest
{
    private static InetAddressAndPort addr;
    private final int messagingVersion;

    private ByteBuf buf;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByName("127.0.0.1");
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Parameters()
    public static Iterable<?> generateData()
    {
        return Arrays.asList(MessagingService.VERSION_30, MessagingService.VERSION_40);
    }

    public MessageInProcessorTest(int messagingVersion)
    {
        this.messagingVersion = messagingVersion;
    }

    @Test(expected = AssertionError.class)
    public void testBadVersionForHandler_Pre40()
    {
        new MessageIn.MessageInProcessorPre40(addr, MessagingService.VERSION_40, (messageIn, integer) -> {});
    }

    @Test(expected = AssertionError.class)
    public void testBadVersionForHandler_40()
    {
        new MessageIn.MessageInProcessorAsOf40(addr, MessagingService.VERSION_30, (messageIn, integer) -> {});
    }

    @Test(expected = IOException.class)
    public void readFirstChunk_BadMagic() throws IOException
    {
        int len = MessageOut.MESSAGE_PREFIX_SIZE;
        buf = Unpooled.buffer(len, len);
        buf.writeInt(-1);
        buf.writerIndex(len);

        MessageIn.MessageInProcessor processor = MessageIn.getProcessor(addr, messagingVersion, (messageIn, integer) -> {});
        processor.readFirstChunk(new ByteBufDataInputPlus(buf));
    }

    @Test
    public void readParameters_WithHalfReceivedParameters() throws Exception
    {
//        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
//        UUID uuid = UUIDGen.getTimeUUID();
//        msgOut = msgOut.withParameter(ParameterType.TRACE_SESSION, uuid);
//
//        serialize(msgOut);
//
//        // move the write index pointer back a few bytes to simulate like the full bytes are not present.
//        // yeah, it's lame, but it tests the basics of what is happening during the deserialiization
//        int originalWriterIndex = buf.writerIndex();
//        buf.writerIndex(originalWriterIndex - 6);
//
//        MessageInHandlerTest.MessageInWrapper wrapper = new MessageInHandlerTest.MessageInWrapper();
//        MessageInHandler handler = getHandler(addr, messagingVersion, wrapper.messageConsumer);
//        handler.channelRead(null, buf);
//
//        Assert.assertNull(wrapper.messageIn);
//
//        MessageIn.MessageHeader header = handler.getMessageHeader();
//        Assert.assertEquals(MSG_ID, header.messageId);
//        Assert.assertEquals(msgOut.verb, header.verb);
//        Assert.assertEquals(msgOut.from, header.from);
//
//        // now, set the writer index back to the original value to pretend that we actually got more bytes in
//        buf.writerIndex(originalWriterIndex);
//        handler.channelRead(null, buf);
//        Assert.assertNotNull(wrapper.messageIn);
    }
}
