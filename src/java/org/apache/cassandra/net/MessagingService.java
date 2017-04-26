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

import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import io.netty.channel.Channel;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.db.Truncation;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.hints.HintMessage;
import org.apache.cassandra.hints.HintResponse;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.net.async.OutboundMessagingPool;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.NettyFactory.InboundInitializer;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.streaming.messages.PrepareAckMessage;
import org.apache.cassandra.streaming.messages.PrepareSynAckMessage;
import org.apache.cassandra.streaming.messages.PrepareSynMessage;
import org.apache.cassandra.streaming.messages.ReceivedMessage;
import org.apache.cassandra.streaming.messages.SessionFailedMessage;
import org.apache.cassandra.streaming.messages.StreamInitAckMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StatusLogger;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public final class MessagingService implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    // 8 bits version, so don't waste versions
    public static final int VERSION_30 = 10;
    public static final int VERSION_40 = 11;
    public static final int current_version = VERSION_40;

    public static final String FAILURE_CALLBACK_PARAM = "CAL_BAC";
    public static final byte[] ONE_BYTE = new byte[1];
    public static final String FAILURE_RESPONSE_PARAM = "FAIL";
    public static final String FAILURE_REASON_PARAM = "FAIL_REASON";

    /**
     * we preface every message with this number so the recipient can validate the sender is sane
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    public final MessagingMetrics metrics = new MessagingMetrics();

    /* All verb handler identifiers */
    public enum Verb
    {
        MUTATION
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },
        HINT
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },
        READ_REPAIR
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },
        READ
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getReadRpcTimeout();
            }
        },
        REQUEST_RESPONSE, // client-initiated reads and writes
        BATCH_STORE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },  // was @Deprecated STREAM_INITIATE,
        BATCH_REMOVE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        }, // was @Deprecated STREAM_INITIATE_DONE,
        @Deprecated STREAM_REPLY,
        @Deprecated STREAM_REQUEST,
        RANGE_SLICE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getRangeRpcTimeout();
            }
        },
        @Deprecated BOOTSTRAP_TOKEN,
        @Deprecated TREE_REQUEST,
        @Deprecated TREE_RESPONSE,
        @Deprecated JOIN,
        GOSSIP_DIGEST_SYN,
        GOSSIP_DIGEST_ACK,
        GOSSIP_DIGEST_ACK2,
        @Deprecated DEFINITIONS_ANNOUNCE,
        DEFINITIONS_UPDATE,
        TRUNCATE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getTruncateRpcTimeout();
            }
        },
        SCHEMA_CHECK,
        @Deprecated INDEX_SCAN,
        REPLICATION_FINISHED,
        INTERNAL_RESPONSE, // responses to internal calls
        COUNTER_MUTATION
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getCounterWriteRpcTimeout();
            }
        },
        @Deprecated STREAMING_REPAIR_REQUEST,
        @Deprecated STREAMING_REPAIR_RESPONSE,
        SNAPSHOT, // Similar to nt snapshot
        MIGRATION_REQUEST,
        GOSSIP_SHUTDOWN,
        _TRACE, // dummy verb so we can use MS.droppedMessagesMap
        ECHO,
        REPAIR_MESSAGE,
        PAXOS_PREPARE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },
        PAXOS_PROPOSE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },
        PAXOS_COMMIT
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getWriteRpcTimeout();
            }
        },
        @Deprecated PAGED_RANGE
        {
            public long getTimeout()
            {
                return DatabaseDescriptor.getRangeRpcTimeout();
            }
        },
        // remember to add new verbs at the end, since we serialize by ordinal
        UNUSED_1,
        UNUSED_2,
        UNUSED_3,
        UNUSED_4,
        UNUSED_5,
        ;

        private int id;
        Verb()
        {
            id = ordinal();
        }

        /**
         * Unused, but it is an extension point for adding custom verbs
         * @param id
         */
        Verb(int id)
        {
            this.id = id;
        }

        public long getTimeout()
        {
            return DatabaseDescriptor.getRpcTimeout();
        }

        public int getId()
        {
            return id;
        }
        private static final IntObjectMap<Verb> idToVerbMap = new IntObjectOpenHashMap<>(values().length);
        static
        {
            for (Verb v : values())
                idToVerbMap.put(v.getId(), v);
        }

        public static Verb fromId(int id)
        {
            return idToVerbMap.get(id);
        }
    }

    public static final EnumMap<MessagingService.Verb, Stage> verbStages = new EnumMap<MessagingService.Verb, Stage>(MessagingService.Verb.class)
    {{
        put(Verb.MUTATION, Stage.MUTATION);
        put(Verb.COUNTER_MUTATION, Stage.COUNTER_MUTATION);
        put(Verb.READ_REPAIR, Stage.MUTATION);
        put(Verb.HINT, Stage.MUTATION);
        put(Verb.TRUNCATE, Stage.MUTATION);
        put(Verb.PAXOS_PREPARE, Stage.MUTATION);
        put(Verb.PAXOS_PROPOSE, Stage.MUTATION);
        put(Verb.PAXOS_COMMIT, Stage.MUTATION);
        put(Verb.BATCH_STORE, Stage.MUTATION);
        put(Verb.BATCH_REMOVE, Stage.MUTATION);

        put(Verb.READ, Stage.READ);
        put(Verb.RANGE_SLICE, Stage.READ);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.PAGED_RANGE, Stage.READ);

        put(Verb.REQUEST_RESPONSE, Stage.REQUEST_RESPONSE);
        put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);

        put(Verb.STREAM_REPLY, Stage.MISC); // actually handled by FileStreamTask and streamExecutors
        put(Verb.STREAM_REQUEST, Stage.MISC);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.SNAPSHOT, Stage.MISC);

        put(Verb.TREE_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.TREE_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.REPAIR_MESSAGE, Stage.ANTI_ENTROPY);
        put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
        put(Verb.GOSSIP_SHUTDOWN, Stage.GOSSIP);

        put(Verb.DEFINITIONS_UPDATE, Stage.MIGRATION);
        put(Verb.SCHEMA_CHECK, Stage.MIGRATION);
        put(Verb.MIGRATION_REQUEST, Stage.MIGRATION);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.SNAPSHOT, Stage.MISC);
        put(Verb.ECHO, Stage.GOSSIP);

        put(Verb.UNUSED_1, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_2, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_3, Stage.INTERNAL_RESPONSE);
    }};

    /**
     * Messages we receive from peers have a Verb that tells us what kind of message it is.
     * Most of the time, this is enough to determine how to deserialize the message payload.
     * The exception is the REQUEST_RESPONSE verb, which just means "a reply to something you told me to do."
     * Traditionally, this was fine since each VerbHandler knew what type of payload it expected, and
     * handled the deserialization itself.  Now that we do that in ITC, to avoid the extra copy to an
     * intermediary byte[] (See CASSANDRA-3716), we need to wire that up to the CallbackInfo object
     * (see below).
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> verbSerializers = new EnumMap<Verb, IVersionedSerializer<?>>(Verb.class)
    {{
        put(Verb.REQUEST_RESPONSE, CallbackDeterminedSerializer.instance);
        put(Verb.INTERNAL_RESPONSE, CallbackDeterminedSerializer.instance);

        put(Verb.MUTATION, Mutation.serializer);
        put(Verb.READ_REPAIR, Mutation.serializer);
        put(Verb.READ, ReadCommand.serializer);
        put(Verb.RANGE_SLICE, ReadCommand.serializer);
        put(Verb.PAGED_RANGE, ReadCommand.serializer);
        put(Verb.BOOTSTRAP_TOKEN, BootStrapper.StringSerializer.instance);
        put(Verb.REPAIR_MESSAGE, RepairMessage.serializer);
        put(Verb.GOSSIP_DIGEST_ACK, GossipDigestAck.serializer);
        put(Verb.GOSSIP_DIGEST_ACK2, GossipDigestAck2.serializer);
        put(Verb.GOSSIP_DIGEST_SYN, GossipDigestSyn.serializer);
        put(Verb.DEFINITIONS_UPDATE, MigrationManager.MigrationsSerializer.instance);
        put(Verb.TRUNCATE, Truncation.serializer);
        put(Verb.REPLICATION_FINISHED, null);
        put(Verb.COUNTER_MUTATION, CounterMutation.serializer);
        put(Verb.SNAPSHOT, SnapshotCommand.serializer);
        put(Verb.ECHO, EchoMessage.serializer);
        put(Verb.PAXOS_PREPARE, Commit.serializer);
        put(Verb.PAXOS_PROPOSE, Commit.serializer);
        put(Verb.PAXOS_COMMIT, Commit.serializer);
        put(Verb.HINT, HintMessage.serializer);
        put(Verb.BATCH_STORE, Batch.serializer);
        put(Verb.BATCH_REMOVE, UUIDSerializer.serializer);
    }};

    /**
     * A Map of what kind of serializer to wire up to a REQUEST_RESPONSE callback, based on outbound Verb.
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> callbackDeserializers = new EnumMap<Verb, IVersionedSerializer<?>>(Verb.class)
    {{
        put(Verb.MUTATION, WriteResponse.serializer);
        put(Verb.HINT, HintResponse.serializer);
        put(Verb.READ_REPAIR, WriteResponse.serializer);
        put(Verb.COUNTER_MUTATION, WriteResponse.serializer);
        put(Verb.RANGE_SLICE, ReadResponse.serializer);
        put(Verb.PAGED_RANGE, ReadResponse.serializer);
        put(Verb.READ, ReadResponse.serializer);
        put(Verb.TRUNCATE, TruncateResponse.serializer);
        put(Verb.SNAPSHOT, null);

        put(Verb.MIGRATION_REQUEST, MigrationManager.MigrationsSerializer.instance);
        put(Verb.SCHEMA_CHECK, UUIDSerializer.serializer);
        put(Verb.BOOTSTRAP_TOKEN, BootStrapper.StringSerializer.instance);
        put(Verb.REPLICATION_FINISHED, null);

        put(Verb.PAXOS_PREPARE, PrepareResponse.serializer);
        put(Verb.PAXOS_PROPOSE, BooleanSerializer.serializer);

        put(Verb.BATCH_STORE, WriteResponse.serializer);
        put(Verb.BATCH_REMOVE, WriteResponse.serializer);
    }};

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo> callbacks;

    /**
     * a placeholder class that means "deserialize using the callback." We can't implement this without
     * special-case code in InboundTcpConnection because there is no way to pass the message id to IVersionedSerializer.
     */
    static class CallbackDeterminedSerializer implements IVersionedSerializer<Object>
    {
        public static final CallbackDeterminedSerializer instance = new CallbackDeterminedSerializer();

        public Object deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public void serialize(Object o, DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(Object o, int version)
        {
            throw new UnsupportedOperationException();
        }
    }

    /* Lookup table for registering message handlers based on the verb. */
    private final Map<Verb, IVerbHandler> verbHandlers;

    @VisibleForTesting
    public final ConcurrentMap<InetAddress, OutboundMessagingPool> channelManagers = new NonBlockingHashMap<>();
    private final List<Channel> serverChannels = Lists.newArrayList();

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    private final SimpleCondition listenGate;

    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    public static final EnumSet<Verb> DROPPABLE_VERBS = EnumSet.of(Verb._TRACE,
                                                                   Verb.MUTATION,
                                                                   Verb.COUNTER_MUTATION,
                                                                   Verb.HINT,
                                                                   Verb.READ_REPAIR,
                                                                   Verb.READ,
                                                                   Verb.RANGE_SLICE,
                                                                   Verb.PAGED_RANGE,
                                                                   Verb.REQUEST_RESPONSE,
                                                                   Verb.BATCH_STORE,
                                                                   Verb.BATCH_REMOVE);

    private static final class DroppedMessages
    {
        final DroppedMessageMetrics metrics;
        final AtomicInteger droppedInternal;
        final AtomicInteger droppedCrossNode;

        DroppedMessages(Verb verb)
        {
            this(new DroppedMessageMetrics(verb));
        }

        DroppedMessages(DroppedMessageMetrics metrics)
        {
            this.metrics = metrics;
            this.droppedInternal = new AtomicInteger(0);
            this.droppedCrossNode = new AtomicInteger(0);
        }
    }

    @VisibleForTesting
    public void resetDroppedMessagesMap(String scope)
    {
        for (Verb verb : droppedMessagesMap.keySet())
            droppedMessagesMap.put(verb, new DroppedMessages(new DroppedMessageMetrics(metricName -> {
                return new CassandraMetricsRegistry.MetricName("DroppedMessages", metricName, scope);
            })));
    }

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedMessages> droppedMessagesMap = new EnumMap<>(Verb.class);

    private final List<ILatencySubscriber> subscribers = new ArrayList<ILatencySubscriber>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, Integer> versions = new NonBlockingHashMap<InetAddress, Integer>();

    // message sinks are a testing hook
    private final Set<IMessageSink> messageSinks = new CopyOnWriteArraySet<>();

    // back-pressure implementation
    private final BackPressureStrategy backPressure = DatabaseDescriptor.getBackPressureStrategy();

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService(false);
    }

    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    private static class MSTestHandle
    {
        public static final MessagingService instance = new MessagingService(true);
    }

    static MessagingService test()
    {
        return MSTestHandle.instance;
    }

    private MessagingService(boolean testOnly)
    {
        for (Verb verb : DROPPABLE_VERBS)
            droppedMessagesMap.put(verb, new DroppedMessages(verb));

        listenGate = new SimpleCondition();
        verbHandlers = new EnumMap<>(Verb.class);
        if (!testOnly)
        {
            Runnable logDropped = new Runnable()
            {
                public void run()
                {
                    logDroppedMessages();
                }
            };
            ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(logDropped, LOG_DROPPED_INTERVAL_IN_MS, LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
        }

        Function<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>>, ?> timeoutReporter = new Function<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>>, Object>()
        {
            public Object apply(Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>> pair)
            {
                final CallbackInfo expiredCallbackInfo = pair.right.value;

                maybeAddLatency(expiredCallbackInfo.callback, expiredCallbackInfo.target, pair.right.timeout);

                ConnectionMetrics.totalTimeouts.mark();
                markTimeout(expiredCallbackInfo.target);

                if (expiredCallbackInfo.callback.supportsBackPressure())
                {
                    updateBackPressureOnReceive(expiredCallbackInfo.target, expiredCallbackInfo.callback, true);
                }

                if (expiredCallbackInfo.isFailureCallback())
                {
                    StageManager.getStage(Stage.INTERNAL_RESPONSE).submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            ((IAsyncCallbackWithFailure)expiredCallbackInfo.callback).onFailure(expiredCallbackInfo.target, RequestFailureReason.UNKNOWN);
                        }
                    });
                }

                if (expiredCallbackInfo.shouldHint())
                {
                    Mutation mutation = ((WriteCallbackInfo) expiredCallbackInfo).mutation();
                    return StorageProxy.submitHint(mutation, expiredCallbackInfo.target, null);
                }

                return null;
            }
        };

        callbacks = new ExpiringMap<>(DatabaseDescriptor.getMinRpcTimeout(), timeoutReporter);

        if (!testOnly)
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            try
            {
                mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void addMessageSink(IMessageSink sink)
    {
        messageSinks.add(sink);
    }

    public void removeMessageSink(IMessageSink sink)
    {
        messageSinks.remove(sink);
    }

    public void clearMessageSinks()
    {
        messageSinks.clear();
    }

    /**
     * Updates the back-pressure state on sending to the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param callback The message callback.
     * @param message The actual message.
     */
    public void updateBackPressureOnSend(InetAddress host, IAsyncCallback callback, MessageOut<?> message)
    {
        if (DatabaseDescriptor.backPressureEnabled() && callback.supportsBackPressure())
        {
            BackPressureState backPressureState = getBackPressureState(host);
            if (backPressureState != null)
                backPressureState.onMessageSent(message);
        }
    }

    /**
     * Updates the back-pressure state on reception from the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param callback The message callback.
     * @param timeout True if updated following a timeout, false otherwise.
     */
    public void updateBackPressureOnReceive(InetAddress host, IAsyncCallback callback, boolean timeout)
    {
        if (DatabaseDescriptor.backPressureEnabled() && callback.supportsBackPressure())
        {
            BackPressureState backPressureState = getBackPressureState(host);
            if (backPressureState == null)
                return;
            if (!timeout)
                backPressureState.onResponseReceived();
            else
                backPressureState.onResponseTimeout();
        }
    }

    /**
     * Applies back-pressure for the given hosts, according to the configured strategy.
     *
     * If the local host is present, it is removed from the pool, as back-pressure is only applied
     * to remote hosts.
     *
     * @param hosts The hosts to apply back-pressure to.
     * @param timeoutInNanos The max back-pressure timeout.
     */
    public void applyBackPressure(Iterable<InetAddress> hosts, long timeoutInNanos)
    {
        if (DatabaseDescriptor.backPressureEnabled())
        {
            Set<BackPressureState> states = new HashSet<BackPressureState>();
            for (InetAddress host : hosts)
            {
                if (host.equals(FBUtilities.getBroadcastAddress()))
                    continue;
                OutboundMessagingPool pool = getMessagingConnection(host);
                if (pool != null)
                    states.add(pool.getBackPressureState());
            }
            backPressure.apply(states, timeoutInNanos, TimeUnit.NANOSECONDS);
        }
    }

    BackPressureState getBackPressureState(InetAddress host)
    {
        OutboundMessagingPool messagingConnection = getMessagingConnection(host);
        return messagingConnection != null ? messagingConnection.getBackPressureState() : null;
    }

    void markTimeout(InetAddress addr)
    {
        OutboundMessagingPool conn = channelManagers.get(addr);
        if (conn != null)
            conn.incrementTimeout();
    }

    /**
     * Track latency information for the dynamic snitch
     *
     * @param cb      the callback associated with this message -- this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     * @param latency
     */
    public void maybeAddLatency(IAsyncCallback cb, InetAddress address, long latency)
    {
        if (cb.isLatencyForSnitch())
            addLatency(address, latency);
    }

    public void addLatency(InetAddress address, long latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(address, latency);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public void convict(InetAddress ep)
    {
        logger.trace("Resetting pool for {}", ep);
        reset(ep);
    }

    public void listen()
    {
        callbacks.reset(); // hack to allow tests to stop/restart MS
        listen(FBUtilities.getLocalAddress());
        if (DatabaseDescriptor.shouldListenOnBroadcastAddress()
            && !FBUtilities.getLocalAddress().equals(FBUtilities.getBroadcastAddress()))
        {
            listen(FBUtilities.getBroadcastAddress());
        }
        listenGate.signalAll();
    }

    /**
     * Listen on the specified port.
     *
     * @param localEp InetAddress whose port to listen on.
     */
    private void listen(InetAddress localEp) throws ConfigurationException
    {
        IInternodeAuthenticator authenticator = DatabaseDescriptor.getInternodeAuthenticator();
        int receiveBufferSize = DatabaseDescriptor.getInternodeRecvBufferSize();
        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.none)
        {
            InetSocketAddress localAddr = new InetSocketAddress(localEp, DatabaseDescriptor.getSSLStoragePort());
            serverChannels.add(NettyFactory.instance.createInboundChannel(localAddr, new InboundInitializer(authenticator, DatabaseDescriptor.getServerEncryptionOptions()), receiveBufferSize));
        }

        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.all)
        {
            InetSocketAddress localAddr = new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort());
            serverChannels.add(NettyFactory.instance.createInboundChannel(localAddr, new InboundInitializer(authenticator, null), receiveBufferSize));
        }

        if (serverChannels.isEmpty())
            throw new IllegalStateException("no listening channels set up in MessagingService!");
    }

    public void waitUntilListening()
    {
        try
        {
            listenGate.await();
        }
        catch (InterruptedException ie)
        {
            logger.trace("await interrupted");
        }
    }

    public boolean isListening()
    {
        return listenGate.isSignaled();
    }


    public void destroyConnectionPool(InetAddress to)
    {
        OutboundMessagingPool pool = channelManagers.remove(to);
        if (pool != null)
            pool.close(true);
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param address IP Address to identify the peer
     * @param preferredAddress IP Address to use (and prefer) going forward for connecting to the peer
     */
    public void reconnectWithNewIp(InetAddress address, InetAddress preferredAddress)
    {
        SystemKeyspace.updatePreferredIP(address, preferredAddress);

        OutboundMessagingPool messagingPool = channelManagers.get(address);
        if (messagingPool != null)
            messagingPool.reconnectWithNewIp(new InetSocketAddress(preferredAddress, portFor(address)));
    }

    private void reset(InetAddress address)
    {
        OutboundMessagingPool messagingPool = channelManagers.remove(address);
        if (messagingPool != null)
            messagingPool.close(false);
    }

    public InetAddress getCurrentEndpoint(InetAddress publicAddress)
    {
        OutboundMessagingPool messagingPool = getMessagingConnection(publicAddress);
        return messagingPool != null ? messagingPool.getPreferredRemoteAddr().getAddress() : null;
    }

    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     *
     * @param verb
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(Verb verb, IVerbHandler verbHandler)
    {
        assert !verbHandlers.containsKey(verb);
        verbHandlers.put(verb, verbHandler);
    }

    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     *
     * @param type for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(Verb type)
    {
        return verbHandlers.get(type);
    }

    public int addCallback(IAsyncCallback cb, MessageOut message, InetAddress to, long timeout, boolean failureCallback)
    {
        assert message.verb != Verb.MUTATION; // mutations need to call the overload with a ConsistencyLevel
        int messageId = nextId();
        CallbackInfo previous = callbacks.put(messageId, new CallbackInfo(to, cb, callbackDeserializers.get(message.verb), failureCallback), timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public int addCallback(IAsyncCallback cb,
                           MessageOut<?> message,
                           InetAddress to,
                           long timeout,
                           ConsistencyLevel consistencyLevel,
                           boolean allowHints)
    {
        assert message.verb == Verb.MUTATION
            || message.verb == Verb.COUNTER_MUTATION
            || message.verb == Verb.PAXOS_COMMIT;
        int messageId = nextId();

        CallbackInfo previous = callbacks.put(messageId,
                                              new WriteCallbackInfo(to,
                                                                    cb,
                                                                    message,
                                                                    callbackDeserializers.get(message.verb),
                                                                    consistencyLevel,
                                                                    allowHints),
                                                                    timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    private static final AtomicInteger idGen = new AtomicInteger(0);

    private static int nextId()
    {
        return idGen.incrementAndGet();
    }

    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        return sendRR(message, to, cb, message.getTimeout(), false);
    }

    public int sendRRWithFailure(MessageOut message, InetAddress to, IAsyncCallbackWithFailure cb)
    {
        return sendRR(message, to, cb, message.getTimeout(), true);
    }

    /**
     * Send a non-mutation message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param cb      callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     * @param timeout the timeout used for expiration
     * @return an reference to message id used to match with the result
     */
    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb, long timeout, boolean failureCallback)
    {
        int id = addCallback(cb, message, to, timeout, failureCallback);
        updateBackPressureOnSend(to, cb, message);
        sendOneWay(failureCallback ? message.withParameter(FAILURE_CALLBACK_PARAM, ONE_BYTE) : message, id, to);
        return id;
    }

    /**
     * Send a mutation message or a Paxos Commit to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * Also holds the message (only mutation messages) to determine if it
     * needs to trigger a hint (uses StorageProxy for that).
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param handler callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public int sendRR(MessageOut<?> message,
                      InetAddress to,
                      AbstractWriteResponseHandler<?> handler,
                      boolean allowHints)
    {
        int id = addCallback(handler, message, to, message.getTimeout(), handler.consistencyLevel, allowHints);
        updateBackPressureOnSend(to, handler, message);
        sendOneWay(message.withParameter(FAILURE_CALLBACK_PARAM, ONE_BYTE), id, to);
        return id;
    }

    public void sendOneWay(MessageOut message, InetAddress to)
    {
        sendOneWay(message, nextId(), to);
    }

    public void sendReply(MessageOut message, int id, InetAddress to)
    {
        sendOneWay(message, id, to);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     *
     * @param message messages to be sent.
     * @param to      endpoint to which the message needs to be sent
     */
    public void sendOneWay(MessageOut message, int id, InetAddress to)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} sending {} to {}@{}", FBUtilities.getBroadcastAddress(), message.verb, id, to);

        if (to.equals(FBUtilities.getBroadcastAddress()))
            logger.trace("Message-to-self {} going over MessagingService", message);

        // message sinks are a testing hook
        for (IMessageSink ms : messageSinks)
            if (!ms.allowOutgoingMessage(message, id, to))
                return;

        OutboundMessagingPool outboundMessagingPool = getMessagingConnection(to);
        if (outboundMessagingPool != null)
            outboundMessagingPool.sendMessage(message, id);
    }

    public <T> AsyncOneResponse<T> sendRR(MessageOut message, InetAddress to)
    {
        AsyncOneResponse<T> iar = new AsyncOneResponse<T>();
        sendRR(message, to, iar);
        return iar;
    }

    public void register(ILatencySubscriber subcriber)
    {
        subscribers.add(subcriber);
    }

    public void clearCallbacksUnsafe()
    {
        callbacks.reset();
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        logger.info("Waiting for messaging service to quiesce");
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !StageManager.getStage(Stage.MUTATION).isShutdown();

        // the important part
        if (!callbacks.shutdownBlocking())
            logger.warn("Failed to wait for messaging service callbacks shutdown");

        // attempt to humor tests that try to stop and restart MS
        try
        {
            // first close the recieve channels
            for (Channel channel : serverChannels)
                channel.close().syncUninterruptibly();

            // now close the send channels
            for (OutboundMessagingPool pool : channelManagers.values())
                pool.close(false);

            NettyFactory.instance.close();
        }
        catch (Exception e)
        {
            throw new IOError(e);
        }
    }

    public void receive(MessageIn message, int id)
    {
        TraceState state = Tracing.instance.initializeFromMessage(message);
        if (state != null)
            state.trace("{} message received from {}", message.verb, message.from);

        // message sinks are a testing hook
        for (IMessageSink ms : messageSinks)
            if (!ms.allowIncomingMessage(message, id))
                return;

        Runnable runnable = new MessageDeliveryTask(message, id);
        LocalAwareExecutorService stage = StageManager.getStage(message.getMessageType());
        assert stage != null : "No stage for message type " + message.verb;

        stage.execute(runnable, ExecutorLocals.create(state));
    }

    public void setCallbackForTests(int messageId, CallbackInfo callback)
    {
        callbacks.put(messageId, callback);
    }

    public CallbackInfo getRegisteredCallback(int messageId)
    {
        return callbacks.get(messageId);
    }

    public CallbackInfo removeRegisteredCallback(int messageId)
    {
        return callbacks.remove(messageId);
    }

    /**
     * @return System.nanoTime() when callback was created.
     */
    public long getRegisteredCallbackAge(int messageId)
    {
        return callbacks.getAge(messageId);
    }

    public static void validateMagic(int magic) throws IOException
    {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }

    public static int getBits(int packed, int start, int count)
    {
        return packed >>> (start + 1) - count & ~(-1 << count);
    }

    /**
     * @return the last version associated with address, or @param version if this is the first such version
     */
    public int setVersion(InetAddress endpoint, int version)
    {
        logger.trace("Setting version {} for {}", version, endpoint);

        Integer v = versions.put(endpoint, version);
        return v == null ? version : v;
    }

    public void resetVersion(InetAddress endpoint)
    {
        logger.trace("Resetting version for {}", endpoint);
        versions.remove(endpoint);
    }

    /**
     * Returns the messaging-version as announced by the given node but capped
     * to the min of the version as announced by the node and {@link #current_version}.
     */
    public int getVersion(InetAddress endpoint)
    {
        Integer v = versions.get(endpoint);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            logger.trace("Assuming current protocol version for {}", endpoint);
            return MessagingService.current_version;
        }
        else
            return Math.min(v, MessagingService.current_version);
    }

    public int getVersion(String endpoint) throws UnknownHostException
    {
        return getVersion(InetAddress.getByName(endpoint));
    }

    /**
     * Returns the messaging-version exactly as announced by the given endpoint.
     */
    public int getRawVersion(InetAddress endpoint)
    {
        Integer v = versions.get(endpoint);
        if (v == null)
            throw new IllegalStateException("getRawVersion() was called without checking knowsVersion() result first");
        return v;
    }

    public boolean knowsVersion(InetAddress endpoint)
    {
        return versions.containsKey(endpoint);
    }

    public void incrementDroppedMutations(Optional<IMutation> mutationOpt, long timeTaken)
    {
        if (mutationOpt.isPresent())
        {
            updateDroppedMutationCount(mutationOpt.get());
        }
        incrementDroppedMessages(Verb.MUTATION, timeTaken);
    }

    public void incrementDroppedMessages(Verb verb)
    {
        incrementDroppedMessages(verb, false);
    }

    public void incrementDroppedMessages(Verb verb, long timeTaken)
    {
        incrementDroppedMessages(verb, timeTaken, false);
    }

    public void incrementDroppedMessages(MessageIn message, long timeTaken)
    {
        if (message.payload instanceof IMutation)
        {
            updateDroppedMutationCount((IMutation) message.payload);
        }
        incrementDroppedMessages(message.verb, timeTaken, message.isCrossNode());
    }

    public void incrementDroppedMessages(Verb verb, long timeTaken, boolean isCrossNode)
    {
        assert DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
        incrementDroppedMessages(droppedMessagesMap.get(verb), timeTaken, isCrossNode);
    }

    public void incrementDroppedMessages(Verb verb, boolean isCrossNode)
    {
        assert DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
        incrementDroppedMessages(droppedMessagesMap.get(verb), isCrossNode);
    }

    private void updateDroppedMutationCount(IMutation mutation)
    {
        assert mutation != null : "Mutation should not be null when updating dropped mutations count";

        for (TableId tableId : mutation.getTableIds())
        {
            ColumnFamilyStore cfs = Keyspace.open(mutation.getKeyspaceName()).getColumnFamilyStore(tableId);
            if (cfs != null)
            {
                cfs.metric.droppedMutations.inc();
            }
        }
    }

    private void incrementDroppedMessages(DroppedMessages droppedMessages, long timeTaken, boolean isCrossNode)
    {
        if (isCrossNode)
            droppedMessages.metrics.crossNodeDroppedLatency.update(timeTaken, TimeUnit.MILLISECONDS);
        else
            droppedMessages.metrics.internalDroppedLatency.update(timeTaken, TimeUnit.MILLISECONDS);
        incrementDroppedMessages(droppedMessages, isCrossNode);
    }

    private void incrementDroppedMessages(DroppedMessages droppedMessages, boolean isCrossNode)
    {
        droppedMessages.metrics.dropped.mark();
        if (isCrossNode)
            droppedMessages.droppedCrossNode.incrementAndGet();
        else
            droppedMessages.droppedInternal.incrementAndGet();
    }

    private void logDroppedMessages()
    {
        List<String> logs = getDroppedMessagesLogs();
        for (String log : logs)
            logger.info(log);

        if (logs.size() > 0)
            StatusLogger.log();
    }

    @VisibleForTesting
    List<String> getDroppedMessagesLogs()
    {
        List<String> ret = new ArrayList<>();
        for (Map.Entry<Verb, DroppedMessages> entry : droppedMessagesMap.entrySet())
        {
            Verb verb = entry.getKey();
            DroppedMessages droppedMessages = entry.getValue();

            int droppedInternal = droppedMessages.droppedInternal.getAndSet(0);
            int droppedCrossNode = droppedMessages.droppedCrossNode.getAndSet(0);
            if (droppedInternal > 0 || droppedCrossNode > 0)
            {
                ret.add(String.format("%s messages were dropped in last %d ms: %d internal and %d cross node."
                                     + " Mean internal dropped latency: %d ms and Mean cross-node dropped latency: %d ms",
                                     verb,
                                     LOG_DROPPED_INTERVAL_IN_MS,
                                     droppedInternal,
                                     droppedCrossNode,
                                     TimeUnit.NANOSECONDS.toMillis((long)droppedMessages.metrics.internalDroppedLatency.getSnapshot().getMean()),
                                     TimeUnit.NANOSECONDS.toMillis((long)droppedMessages.metrics.crossNodeDroppedLatency.getSnapshot().getMean())));
            }
        }
        return ret;
    }


    private static void handleIOExceptionOnClose(IOException e) throws IOException
    {
        // dirty hack for clean shutdown on OSX w/ Java >= 1.8.0_20
        // see https://bugs.openjdk.java.net/browse/JDK-8050499;
        // also CASSANDRA-12513
        if (NativeLibrary.osType == NativeLibrary.OSType.MAC)
        {
            switch (e.getMessage())
            {
                case "Unknown error: 316":
                case "No such file or directory":
                    return;
            }
        }

        throw e;
    }

    public Map<String, Integer> getLargeMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().largeMessageChannel.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getLargeMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().largeMessageChannel.getCompletedMessages());
        return completedTasks;
    }

    public Map<String, Long> getLargeMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().largeMessageChannel.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getSmallMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().smallMessageChannel.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getSmallMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().smallMessageChannel.getCompletedMessages());
        return completedTasks;
    }

    public Map<String, Long> getSmallMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().smallMessageChannel.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getGossipMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossipChannel.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getGossipMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossipChannel.getCompletedMessages());
        return completedTasks;
    }

    public Map<String, Long> getGossipMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossipChannel.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<>(droppedMessagesMap.size());
        for (Map.Entry<Verb, DroppedMessages> entry : droppedMessagesMap.entrySet())
            map.put(entry.getKey().toString(), (int) entry.getValue().metrics.dropped.getCount());
        return map;
    }


    public long getTotalTimeouts()
    {
        return ConnectionMetrics.totalTimeouts.getCount();
    }

    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
        {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getTimeouts();
            result.put(ip, recent);
        }
        return result;
    }

    public Map<String, Double> getBackPressurePerHost()
    {
        Map<String, Double> map = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            map.put(entry.getKey().getHostAddress(), entry.getValue().getBackPressureState().getBackPressureRateLimit());

        return map;
    }

    @Override
    public void setBackPressureEnabled(boolean enabled)
    {
        DatabaseDescriptor.setBackPressureEnabled(enabled);
    }

    @Override
    public boolean isBackPressureEnabled()
    {
        return DatabaseDescriptor.backPressureEnabled();
    }

    public static IPartitioner globalPartitioner()
    {
        return StorageService.instance.getTokenMetadata().partitioner;
    }

    public static void validatePartitioner(Collection<? extends AbstractBounds<?>> allBounds)
    {
        for (AbstractBounds<?> bounds : allBounds)
            validatePartitioner(bounds);
    }

    public static void validatePartitioner(AbstractBounds<?> bounds)
    {
        if (globalPartitioner() != bounds.left.getPartitioner())
            throw new AssertionError(String.format("Partitioner in bounds serialization. Expected %s, was %s.",
                                                   globalPartitioner().getClass().getName(),
                                                   bounds.left.getPartitioner().getClass().getName()));
    }

    private OutboundMessagingPool getMessagingConnection(InetAddress to)
    {
        OutboundMessagingPool pool = channelManagers.get(to);
        if (pool == null)
        {
            final boolean secure = isEncryptedConnection(to);
            final int port = portFor(secure);
            if (!DatabaseDescriptor.getInternodeAuthenticator().authenticate(to, port))
                return null;

            InetSocketAddress preferredRemote = new InetSocketAddress(SystemKeyspace.getPreferredIP(to), port);
            InetSocketAddress local = new InetSocketAddress(FBUtilities.getLocalAddress(), 0);
            ServerEncryptionOptions encryptionOptions = secure ? DatabaseDescriptor.getServerEncryptionOptions() : null;
            IInternodeAuthenticator authenticator = DatabaseDescriptor.getInternodeAuthenticator();

            pool = new OutboundMessagingPool(preferredRemote, local, encryptionOptions, backPressure.newState(to), authenticator);
            OutboundMessagingPool existing = channelManagers.putIfAbsent(to, pool);
            if (existing != null)
            {
                pool.close(false);
                pool = existing;
            }
        }
        return pool;
    }

    public static int portFor(InetAddress addr)
    {
        final boolean secure = isEncryptedConnection(addr);
        return portFor(secure);
    }

    private static int portFor(boolean secure)
    {
        return secure ? DatabaseDescriptor.getSSLStoragePort() : DatabaseDescriptor.getStoragePort();
    }

    @VisibleForTesting
    boolean isConnected(InetAddress address, MessageOut messageOut)
    {
        OutboundMessagingPool pool = channelManagers.get(address);
        if (pool == null)
            return false;
        return pool.getConnection(messageOut).isConnected();
    }

    public static boolean isEncryptedConnection(InetAddress address)
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        switch (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption)
        {
            case none:
                return false; // if nothing needs to be encrypted then return immediately.
            case all:
                break;
            case dc:
                if (snitch.getDatacenter(address).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
            case rack:
                // for rack then check if the DC's are the same.
                if (snitch.getRack(address).equals(snitch.getRack(FBUtilities.getBroadcastAddress()))
                    && snitch.getDatacenter(address).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
        }
        return true;
    }
}
