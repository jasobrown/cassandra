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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.InetAddressAndPort;

public class MessageInHandler extends ChannelInboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    final InetAddressAndPort peer;

    private BufferHandler bufferHandler;
    private final MessageInProcessor messageProcessor;
    private final boolean handlesLargeMessages;

    public MessageInHandler(InetAddressAndPort peer, MessageInProcessor messageProcessor, boolean handlesLargeMessages)
    {
        this.peer = peer;
        this.messageProcessor = messageProcessor;
        this.handlesLargeMessages = handlesLargeMessages;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        if (handlesLargeMessages)
            bufferHandler = new BackgroundBufferHandler(ctx, messageProcessor);
        else
            bufferHandler = new ForegroundBufferHandler(messageProcessor);

        ctx.fireChannelActive();
    }


    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        bufferHandler.channelRead(ctx, (ByteBuf) msg);
    }

    // TODO:JEB reevaluate the error handling once I switch from ByteToMessageDecoder to ChannelInboundHandlerAdapter
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

    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.trace("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        ctx.fireChannelInactive();
    }

    interface BufferHandler
    {
        void channelRead(ChannelHandlerContext ctx, ByteBuf in) throws IOException;

        void close();
    }

    class ForegroundBufferHandler implements BufferHandler
    {
        private final MessageInProcessor messageProcessor;
        /**
         * If a buffer is not completely consumed, stash it here for the next invocation of
         * {@link #channelRead(ChannelHandlerContext, ByteBuf)}.
         */
        private ByteBuf retainedInlineBuffer;

        ForegroundBufferHandler(MessageInProcessor messageProcessor)
        {
            this.messageProcessor = messageProcessor;
        }

        public void channelRead(ChannelHandlerContext ctx, ByteBuf in)
        {
            final ByteBuf toProcess;
            if (retainedInlineBuffer != null)
                toProcess = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(), retainedInlineBuffer, in);
            else
                toProcess = in;

            try
            {
                messageProcessor.process(toProcess);
            }
            catch (Throwable cause)
            {
                exceptionCaught(ctx, cause);
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

        public void close()
        {
            retainedInlineBuffer.release();
            retainedInlineBuffer = null;
        }
    }

    class BackgroundBufferHandler implements BufferHandler
    {
        /**
         * The default low-water mark to set on {@link #queuedBuffers}.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final int OFFLINE_QUEUE_LOW_WATER_MARK = 1 << 14;

        /**
         * The default high-water mark to set on {@link #queuedBuffers}.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final int OFFLINE_QUEUE_HIGH_WATER_MARK = 1 << 15;

        /**
         * A queue in which to stash incoming {@link ByteBuf}s.
         */
        private final RebufferingByteBufDataInputPlus queuedBuffers;
        private final MessageInProcessor messageProcessor;

        private volatile boolean closed;

        BackgroundBufferHandler(ChannelHandlerContext ctx, MessageInProcessor messageProcessor)
        {
            queuedBuffers = new RebufferingByteBufDataInputPlus(OFFLINE_QUEUE_LOW_WATER_MARK,
                                                                OFFLINE_QUEUE_HIGH_WATER_MARK,
                                                                ctx.channel().config());
            this.messageProcessor = messageProcessor;

            Thread blockingIOThread = new FastThreadLocalThread(() -> processInBackground(ctx));
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

        private void processInBackground(ChannelHandlerContext ctx)
        {
            try
            {
                messageProcessor.process(queuedBuffers);
            }
            catch (Throwable cause)
            {
                exceptionCaught(ctx, cause);
            }
            finally
            {
                if (queuedBuffers != null)
                    queuedBuffers.close();
            }
        }

        public void close()
        {
            closed = true;
            queuedBuffers.markClose();
        }
    }
}
