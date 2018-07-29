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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;

public abstract class BaseMessageInHandler extends ChannelInboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(BaseMessageInHandler.class);

    enum State
    {
        READ_FIRST_CHUNK,
        READ_IP_ADDRESS,
        READ_VERB,
        READ_PARAMETERS_SIZE,
        READ_PARAMETERS_DATA,
        READ_PAYLOAD_SIZE,
        READ_PAYLOAD
    }

    /**
     * The byte count for magic, msg id, timestamp values.
     */
    @VisibleForTesting
    static final int FIRST_SECTION_BYTE_COUNT = 12;

    static final int VERB_LENGTH = Integer.BYTES;

    /**
     * The default target for consuming deserialized {@link MessageIn}.
     */
    static final BiConsumer<MessageIn, Integer> MESSAGING_SERVICE_CONSUMER = (messageIn, id) -> MessagingService.instance().receive(messageIn, id);

    /**
     * Abstracts out depending directly on {@link MessagingService#receive(MessageIn, int)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    final BiConsumer<MessageIn, Integer> messageConsumer;

    final BufferHandler bufferHandler;
    final InetAddressAndPort peer;
    final int messagingVersion;

    public BaseMessageInHandler(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        this.peer = peer;
        this.messagingVersion = messagingVersion;
        this.messageConsumer = messageConsumer;

        if (largeMessages)
            bufferHandler = new BackgroundBufferHandler(ctx);
        else
            bufferHandler = new ForegroundBufferHandler();
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        bufferHandler.channelRead(ctx, );
    }

    // TODO"JEB rename me
    abstract void decode(ChannelHandlerContext ctx, ByteBuf in);

    MessageHeader readFirstChunk(ByteBuf in) throws IOException
    {
        if (in.readableBytes() < FIRST_SECTION_BYTE_COUNT)
            return null;
        MessagingService.validateMagic(in.readInt());
        MessageHeader messageHeader = new MessageInHandler.MessageHeader();
        messageHeader.messageId = in.readInt();
        int messageTimestamp = in.readInt(); // make sure to read the sent timestamp, even if DatabaseDescriptor.hasCrossNodeTimeout() is not enabled
        messageHeader.constructionTime = MessageIn.deriveConstructionTime(peer, messageTimestamp, ApproximateTime.currentTimeMillis());

        return messageHeader;
    }

    // TODO:JEB reevaluate the error handling once I switch from ByteToMessageDecoder to ChannelInboundHandlerAdapter
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn("Got message from unknown table while reading from socket; closing", cause);
        else if (cause instanceof IOException)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn("Unexpected exception caught in inbound channel pipeline from " + ctx.channel().remoteAddress(), cause);

        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.trace("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        ctx.fireChannelInactive();
    }

    // should ony be used for testing!!!
    @VisibleForTesting
    abstract MessageHeader getMessageHeader();

    /**
     * A simple struct to hold the message header data as it is being built up.
     */
    static class MessageHeader
    {
        int messageId;
        long constructionTime;
        InetAddressAndPort from;
        MessagingService.Verb verb;
        int payloadSize;

        Map<ParameterType, Object> parameters = Collections.emptyMap();

        /**
         * Length of the parameter data. If the message's version is {@link MessagingService#VERSION_40} or higher,
         * this value is the total number of header bytes; else, for legacy messaging, this is the number of
         * key/value entries in the header.
         */
        int parameterLength;
    }

    // TODO:JEB document this -- basically, a poor coder's mixin
    abstract class BufferHandler
    {
        abstract void channelRead(ChannelHandlerContext ctx, ByteBuf in);

        abstract void close()
    }

    class ForegroundBufferHandler extends BufferHandler
    {
        /**
         * If a buffer is not completely consumed, stash it here for the next invocation of
         * {@link #channelRead(ChannelHandlerContext, ByteBuf)}.
         */
        private ByteBuf retainedInlineBuffer;

        void channelRead(ChannelHandlerContext ctx, ByteBuf in)
        {
            final ByteBuf toProcess;
            if (retainedInlineBuffer != null)
                toProcess = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(), retainedInlineBuffer, in);
            else
                toProcess = in;

            try
            {
                process(ctx, toProcess);
            }
            finally
            {
                if (toProcess.isReadable())
                {
                    retainedInlineBuffer = toProcess;
                }
                else
                {
                    toProcess.release();
                    retainedInlineBuffer = null;
                }
            }
        }

        void close()
        {
            retainedInlineBuffer.release();
            retainedInlineBuffer = null;
        }
    }

    class BackgroundBufferHandler extends BufferHandler
    {
        /**
         * The default low-water mark to set on {@link #queuedBuffers} when in {@link Mode#OFFLOAD}.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final int OFFLINE_QUEUE_LOW_WATER_MARK = 1 << 14;

        /**
         * The default high-water mark to set on {@link #queuedBuffers} when in {@link Mode#OFFLOAD}.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final int OFFLINE_QUEUE_HIGH_WATER_MARK = 1 << 15;

        /**
         * A queue in which to stash incoming {@link ByteBuf}s.
         */
        private final RebufferingByteBufDataInputPlus queuedBuffers;

        private volatile boolean closed;

        BackgroundBufferHandler(ChannelHandlerContext ctx)
        {
            queuedBuffers = new RebufferingByteBufDataInputPlus(OFFLINE_QUEUE_LOW_WATER_MARK,
                                                                OFFLINE_QUEUE_HIGH_WATER_MARK,
                                                                ctx.channel().config());

            Thread blockingIOThread = new FastThreadLocalThread(() -> process(ctx));
            blockingIOThread.setDaemon(true);
            blockingIOThread.start();
        }

        public void channelRead(ChannelHandlerContext ctx, ByteBuf in)
        {
            // TODO:JEB perhaps put this in a more general location
            if (closed)
            {
                in.release();
                return;
            }

            queuedBuffers.append(in);
        }

        private void process(ChannelHandlerContext ctx)
        {
            try
            {
                while (!closed)
                {
                    // TODO:JEB this is the blocking version of what's in the MIH.decode() functions.
                    // Is there a way to make either this function or what's in MIH.decode() functions generic?
                    // I think that needs to happen, or else it's gonna get ugly ...

                    // also, in the current state, this is completely fucking broken
                    MessagingService.validateMagic(queuedBuffers.readInt());
                    int messageId = queuedBuffers.readInt();
                    int messageTimestamp = queuedBuffers.readInt(); // make sure to read the sent timestamp, even if DatabaseDescriptor.hasCrossNodeTimeout() is not enabled
                    long constructionTime = MessageIn.deriveConstructionTime(peer, messageTimestamp, ApproximateTime.currentTimeMillis());

                    MessageIn messageIn = MessageIn.read(queuedBuffers, messagingVersion, messageId, constructionTime);
                    if (messageIn != null)
                        messageConsumer.accept(messageIn, messageId);
                }
            }
            catch (EOFException eof)
            {
                // ignore
            }
            catch(Throwable t)
            {
                exceptionCaught(ctx, t);
            }
            finally
            {
                if (queuedBuffers != null)
                    queuedBuffers.close();
            }
        }

        void close()
        {
            closed = true;
            queuedBuffers.markClose();
        }
    }
}
