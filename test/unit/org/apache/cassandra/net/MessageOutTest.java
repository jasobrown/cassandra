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

import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut.MessageOutSizes;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

public class MessageOutTest
{
    private static InetAddressAndPort addr;
    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.73.101"));
    }

    @Test
    public void testSerializedSizeBuilder()
    {
        int headersSize = 17;
        int payloadSize = 242;
        int version = MessagingService.current_version;
        MessageOutSizes sizes = new MessageOutSizes(headersSize, payloadSize);
        long serializedSize = MessageOut.buildSerializedSizeValue(sizes, version);

        Assert.assertEquals(version, MessageOut.getMessagingVersionFromSerializedSize(serializedSize));
        Assert.assertEquals(payloadSize, MessageOut.getPayloadSizeFromSerializedSize(serializedSize));
        Assert.assertEquals(headersSize + payloadSize, MessageOut.getMessageSizeFromSerializedSize(serializedSize));
    }

    @Test
    public void serializedSize()
    {
        UUID uuid = UUIDGen.getTimeUUID();
        Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);
        parameters.put(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);
        parameters.put(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
        parameters.put(ParameterType.TRACE_SESSION, uuid);
        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        for (Map.Entry<ParameterType, Object> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());

        Assert.assertEquals(MessageOut.UNINITIALIZED_SERIALIZED_SIZE, msgOut.getSerializedSize());
        long serializedSize40 = msgOut.serializedSize(MessagingService.VERSION_40);
        Assert.assertEquals(serializedSize40, MessageOut.getMessageSizeFromSerializedSize(msgOut.getSerializedSize()));
        long serializedSizePre40 = msgOut.serializedSize(MessagingService.VERSION_30);
        Assert.assertNotEquals(serializedSize40, serializedSizePre40);

        // fetch again, to make sure cache works
        long serializedSize40Again = msgOut.serializedSize(MessagingService.VERSION_40);
        Assert.assertEquals(serializedSize40, serializedSize40Again);
    }
}
