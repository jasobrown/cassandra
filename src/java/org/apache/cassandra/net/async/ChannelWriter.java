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
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MessageSizeEstimator;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Represents a ready and post-handshake channel that can send outbound messages. This class groups a netty channel
 * with any other channel-related information we track and, most importantly, handles the details on when the channel is flushed.
 *
 * <h2>Flushing</h2>
 *
 * We don't flush to the socket on every message as it's a bit of a performance drag (making the system call, copying
 * the buffer, sending out a small packet). Thus, by waiting until we have a decent chunk of data (for some definition
 * of 'decent'), we can achieve better efficiency and improved performance (yay!).
 * <p>
 * When to flush mainly depends on whether we use message coalescing or not (see {@link CoalescingStrategies}).
 * <p>
 * Note that the callback functions are invoked on the netty event loop, which is (in almost all cases) different
 * from the thread that will be invoking {@link #write(QueuedMessage)}.
 *
 * <h3>Flushing without coalescing</h3>
 *
 * When no coalescing is in effect, we want to send new message "right away". However, as said above, flushing after
 * every message would be particularly inefficient when there is lots of message in our sending queue, and so in
 * practice we want to flush in 2 cases:
 *  1) After any message <b>if</b> there is no pending message in the send queue.
 *  2) When we've filled up or exceeded the netty outbound buffer (see {@link ChannelOutboundBuffer})
 * <p>
 * The second part is relatively simple and handled generically in {@link MessageOutHandler#write(ChannelHandlerContext, Object, ChannelPromise)} [1].
 * The first part however is made a little more complicated by how netty's event loop executes. It is woken up by
 * external callers to the channel invoking a flush, via either {@link Channel#flush} or one of the {@link Channel#writeAndFlush}
 * methods [2]. So a plain {@link Channel#write} will only queue the message in the channel, and not wake up the event loop.
 * <p>
 * This means we don't want to simply call {@link Channel#write} as we want the message processed immediately. But we
 * also don't want to flush on every message if there is more in the sending queue, so simply calling
 * {@link Channel#writeAndFlush} isn't completely appropriate either. In practice, we handle this by calling
 * {@link Channel#writeAndFlush} (so the netty event loop <b>does</b> wake up), but we override the flush behavior so
 * it actually only flushes if there are no pending messages (see how {@link MessageOutHandler#flush} delegates the flushing
 * decision back to this class through {@link #onTriggeredFlush}, and how {@link SimpleChannelWriter} makes this a no-op;
 * instead {@link SimpleChannelWriter} flushes after any message if there are no more pending ones in
 * {@link #onMessageProcessed}).
 *
 * <h3>Flushing with coalescing</h3>
 *
 * The goal of coalescing is to (artificially) delay the flushing of data in order to aggregate even more data before
 * sending a group of packets out. So we don't want to flush after messages even if there is no pending messages in the
 * sending queue, but we rather want to delegate the decision on when to flush to the {@link CoalescingStrategy}. In
 * pratice, when coalescing is enabled we will flush in 2 cases:
 *  1) When the coalescing strategies decides that we should.
 *  2) When we've filled up or exceeded the netty outbound buffer ({@link ChannelOutboundBuffer}), exactly like in the
 *  no coalescing case.
 *  <p>
 *  The second part is handled exactly like in the no coalescing case, see above.
 *  The first part is handled by {@link CoalescingChannelWriter#write(QueuedMessage)}. Whenever a message is sent, we check
 *  if a flush has been already scheduled by the coalescing strategy. If one has, we're done, otherwise we ask the
 *  strategy when the next flush should happen and schedule one.
 *
 *<h2>Message timeouts and retries</h2>
 *
 * The main outward-facing method is {@link #write(QueuedMessage)}, where callers pass a
 * {@link QueuedMessage}. If a message times out, as defined in {@link QueuedMessage#isTimedOut()},
 * the message listener {@link #handleMessageFuture(Future, QueuedMessage, boolean)} is invoked
 * with the cause being a {@link ExpiredException}. The message is not retried and it is dropped on the floor.
 * <p>
 * If there is some {@link IOException} on the socket after the message has been written to the netty channel,
 * the message listener {@link #handleMessageFuture(Future, QueuedMessage, boolean)} is invoked
 * and 1) we check to see if the connection should be re-established, and 2) possibly createRetry the message.
 *
 * <h2>Failures</h2>
 *
 * <h3>Failure to make progress sending bytes</h3>
 * If we are unable to make progress sending messages, we'll receive a netty notification
 * ({@link IdleStateEvent}) at {@link MessageOutHandler#userEventTriggered(ChannelHandlerContext, Object)}.
 * We then want to close the socket/channel, and purge any messages in {@link OutboundMessagingConnection#backlog}
 * to try to free up memory as quickly as possible. Any messages in the netty pipeline will be marked as fail
 * (as we close the channel), but {@link MessageOutHandler#userEventTriggered(ChannelHandlerContext, Object)} also
 * sets a channel attribute, {@link #PURGE_MESSAGES_CHANNEL_ATTR} to true. This is essentially as special flag
 * that we can look at in the promise handler code ({@link #handleMessageFuture(Future, QueuedMessage, boolean)})
 * to indicate that any backlog should be thrown away.
 *
 * <h2>Notes</h2>
 * [1] For those desperately interested, and only after you've read the entire class-level doc: You can register a custom
 * {@link MessageSizeEstimator} with a netty channel. When a message is written to the channel, it will check the
 * message size, and if the max ({@link ChannelOutboundBuffer}) size will be exceeded, a task to signal the "channel
 * writability changed" will be executed in the channel. That task, however, will wake up the event loop.
 * Thus if coalescing is enabled, the event loop will wake up prematurely and process (and possibly flush!) the messages
 * currently in the queue, thus defeating an aspect of coalescing. Hence, we're not using that feature of netty.
 * [2]: The netty event loop is also woken up by it's internal timeout on the epoll_wait() system call.
 */
abstract class ChannelWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ChannelWriter.class);

    protected final Channel channel;
    private volatile boolean closed;

    protected ChannelWriter(Channel channel)
    {
        this.channel = channel;
    }

    /**
     * Creates a new {@link ChannelWriter} using the (assumed properly connected) provided channel, and using coalescing
     * based on the provided strategy.
     */
    static ChannelWriter create(Channel channel, OutboundConnectionParams params)
    {
        return params.coalescingStrategy != null
               ? new CoalescingChannelWriter(channel, params.coalescingStrategy)
               : new SimpleChannelWriter(channel);
    }

    /**
     * Writes a message to the {@link #channel}. If the channel is closed, the promise will be notifed as a fail
     * (due to channel closed), and let the any listeners on the promise do the reconnect magic/dance.
     *
     * @param message the message to write/send.
     */
    abstract ChannelFuture write(QueuedMessage message);

    abstract boolean flush(int remainingBacklogSize);

    void close()
    {
        if (closed)
            return;

        closed = true;
        channel.close();
    }

    /**
     * Close the underlying channel but only after having make sure every pending message has been properly sent.
     */
    void softClose()
    {
        if (closed)
            return;

        closed = true;
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @VisibleForTesting
    boolean isClosed()
    {
        return closed || !channel.isOpen();
    }

    /**
     * Handles the non-coalescing flush case.
     */
    @VisibleForTesting
    static class SimpleChannelWriter extends ChannelWriter
    {
        private SimpleChannelWriter(Channel channel)
        {
            super(channel);
        }

        protected ChannelFuture write(QueuedMessage message)
        {
            return channel.write(message);
        }

        protected boolean flush(int remainingBacklogSize)
        {
            if (remainingBacklogSize == 0)
            {
                logger.debug("{} JEB::CW::simple::doFLush - sending flush!! {}, backlogSize = {}", channel.id(), remainingBacklogSize);
                channel.flush();
                return true;
            }

            return false;
        }
    }

    /**
     * Handles the coalescing flush case.
     */
    @VisibleForTesting
    static class CoalescingChannelWriter extends ChannelWriter
    {
        private static final int MIN_MESSAGES_FOR_COALESCE = DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages();

        private final CoalescingStrategy strategy;
        private final int minMessagesForCoalesce;

        // TODO:JEB doc me
        private int messagesSinceFlush;
        private ScheduledFuture<?> scheduledFlush;

        CoalescingChannelWriter(Channel channel, CoalescingStrategy strategy)
        {
            this (channel, strategy, MIN_MESSAGES_FOR_COALESCE);
        }

        @VisibleForTesting
        CoalescingChannelWriter(Channel channel, CoalescingStrategy strategy, int minMessagesForCoalesce)
        {
            super(channel);
            this.strategy = strategy;
            this.minMessagesForCoalesce = minMessagesForCoalesce;
        }

        protected ChannelFuture write(QueuedMessage message)
        {
            ChannelFuture future = channel.write(message);
            strategy.newArrival(message);
            messagesSinceFlush++;
            return future;
        }

        protected boolean flush(int remainingBacklogSize)
        {
            long flushDelayNanos;
            // if we've hit the minimum number of messages for coalescing or we've run out of coalesce time, flush.
            // TODO:JEB compare me with 3.11
            if (messagesSinceFlush == minMessagesForCoalesce || (flushDelayNanos = strategy.currentCoalescingTimeNanos()) <= 0)
            {
                doFlush();
                return true;
            }
            else if (scheduledFlush != null)
            {
                scheduledFlush = channel.eventLoop().schedule(this::doFlush, flushDelayNanos, TimeUnit.NANOSECONDS);
            }

            return false;
        }

        private void doFlush()
        {
            channel.flush();
            messagesSinceFlush = 0;
            scheduledFlush = null;
        }

        void close()
        {
            super.close();
            if (scheduledFlush != null)
            {
                scheduledFlush.cancel(false);
                scheduledFlush = null;
            }
        }
    }
}
