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
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

/**
 *  A wrapper for outbound messages. All messages will be retried once.
 */
public class QueuedMessage implements CoalescingStrategies.Coalescable
{
    private static final Logger logger = LoggerFactory.getLogger(QueuedMessage.class);

    public final MessageOut<?> message;
    public final int id;
    public final boolean droppable;
    private final long timestampNanos;
    private final boolean retryable;

    public QueuedMessage(MessageOut<?> message, int id)
    {
        this(message, id, System.nanoTime(), MessagingService.DROPPABLE_VERBS.contains(message.verb), true);
    }

    @VisibleForTesting
    public QueuedMessage(MessageOut<?> message, int id, long timestampNanos, boolean droppable, boolean retryable)
    {
        this.message = message;
        this.id = id;
        this.timestampNanos = timestampNanos;
        this.droppable = droppable;
        this.retryable = retryable;
    }

    /** don't drop a non-droppable message just because it's timestamp is expired */
    public boolean isTimedOut()
    {
        return droppable && timestampNanos < System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(message.getTimeout());
    }

    public boolean shouldRetry()
    {
        return retryable;
    }

    public QueuedMessage createRetry()
    {
        return new QueuedMessage(message, id, System.nanoTime(), droppable, false);
    }

    public long timestampNanos()
    {
        return timestampNanos;
    }


    public void serialize(DataOutputPlus out, int messagingVersion, OutboundConnectionIdentifier destinationId) throws IOException
    {
        captureTracingInfo(messagingVersion, destinationId);
        serializeMessage(out, messagingVersion);
    }

    /**
     * Record any tracing data, if enabled on this message.
     */
    @VisibleForTesting
    void captureTracingInfo(int messagingVersion, OutboundConnectionIdentifier destinationId)
    {
        try
        {
            UUID sessionId =  (UUID)message.getParameter(ParameterType.TRACE_SESSION);
            if (sessionId != null)
            {
                TraceState state = Tracing.instance.get(sessionId);
                String logMessage = String.format("Sending %s message to %s, size = %d bytes",
                                               message.verb, destinationId.connectionAddress(),
                                               message.serializedSize(messagingVersion) + MessageOutHandler.MESSAGE_PREFIX_SIZE);
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    Tracing.TraceType traceType = (Tracing.TraceType)message.getParameter(ParameterType.TRACE_TYPE);
                    traceType = traceType == null ? Tracing.TraceType.QUERY : traceType;
                    Tracing.instance.trace(ByteBuffer.wrap(UUIDGen.decompose(sessionId)), logMessage, traceType.getTTL());
                }
                else
                {
                    state.trace(logMessage);
                    if (message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("failed to capture the tracing info for an outbound message to {}, ignoring", destinationId, e);
        }
    }

    private void serializeMessage(DataOutputPlus out, int messagingVersion) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        out.writeInt(id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) NanoTimeToCurrentTimeMillis.convert(timestampNanos));
        message.serialize(out, messagingVersion);
    }
}
