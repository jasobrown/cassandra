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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.net.async.ByteBufDataOutputPlus;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

@State(Scope.Thread)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.SampleTime)
public class MessageOutBench
{

    @Param({ "true", "false" })
    private boolean withParams;

    private MessageOut msgOut;
    private ByteBuf buf;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        InetAddressAndPort addr = InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.73.101"));

        UUID uuid = UUIDGen.getTimeUUID();
        Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);

        if (withParams)
        {
            parameters.put(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);
            parameters.put(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
            parameters.put(ParameterType.TRACE_SESSION, uuid);
        }

        msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
    }

    @Benchmark
    public int serialize40() throws IOException
    {
        return serialize(MessagingService.VERSION_40);
    }

    private int serialize(int messagingVersion) throws IOException
    {
        buf.resetWriterIndex();
        buf.resetReaderIndex();
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(42); // this is the id
        buf.writeInt((int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime()));

        msgOut.serialize(new ByteBufDataOutputPlus(buf), messagingVersion);
        return buf.readableBytes();
    }

    @Benchmark
    public int serializePre40() throws IOException
    {
        return serialize(MessagingService.VERSION_30);
    }
}
