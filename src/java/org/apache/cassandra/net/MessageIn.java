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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.net.async.ByteBufDataInputPlus;
import org.apache.cassandra.net.async.RebufferingByteBufDataInputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * The receiving node's view of a {@link MessageOut}. See documentation on {@link MessageOut} for details on the
 * serialization format.
 *
 * @param <T> The type of the payload
 */
public class MessageIn<T>
{
    public final InetAddressAndPort from;
    public final T payload;
    public final Map<ParameterType, Object> parameters;
    public final MessagingService.Verb verb;
    public final int version;
    public final long constructionTime;


    private MessageIn(InetAddressAndPort from,
                      T payload,
                      Map<ParameterType, Object> parameters,
                      Verb verb,
                      int version,
                      long constructionTime)
    {
        this.from = from;
        this.payload = payload;
        this.parameters = parameters;
        this.verb = verb;
        this.version = version;
        this.constructionTime = constructionTime;
    }

    public static <T> MessageIn<T> create(InetAddressAndPort from,
                                          T payload,
                                          Map<ParameterType, Object> parameters,
                                          Verb verb,
                                          int version,
                                          long constructionTime)
    {
        return new MessageIn<>(from, payload, parameters, verb, version, constructionTime);
    }

    public static <T> MessageIn<T> create(InetAddressAndPort from,
                                          T payload,
                                          Map<ParameterType, Object> parameters,
                                          MessagingService.Verb verb,
                                          int version)
    {
        return new MessageIn<>(from, payload, parameters, verb, version, ApproximateTime.currentTimeMillis());
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id) throws IOException
    {
        return read(in, version, id, ApproximateTime.currentTimeMillis());
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id, long constructionTime) throws IOException
    {
        InetAddressAndPort from = CompactEndpointSerializationHelper.instance.deserialize(in, version);

        MessagingService.Verb verb = MessagingService.Verb.fromId(in.readInt());
        Map<ParameterType, Object> parameters = readParameters(in, version);
        int payloadSize = in.readInt();
        return read(in, version, id, constructionTime, from, payloadSize, verb, parameters);
    }

    private static Map<ParameterType, Object> readParameters(DataInputPlus in, int version) throws IOException
    {
        int parameterCount = in.readInt();
        if (parameterCount == 0)
        {
            return Collections.emptyMap();
        }
        else
        {
            ImmutableMap.Builder<ParameterType, Object> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++)
            {
                String key = in.readUTF();
                ParameterType type = ParameterType.byName.get(key);
                if (type != null)
                {
                    byte[] value = new byte[in.readInt()];
                    in.readFully(value);
                    try (DataInputBuffer buffer = new DataInputBuffer(value))
                    {
                        builder.put(type, type.serializer.deserialize(buffer, version));
                    }
                }
                else
                {
                    in.skipBytes(in.readInt());
                }
            }
            return builder.build();
        }
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id, long constructionTime,
                                          InetAddressAndPort from, int payloadSize, Verb verb, Map<ParameterType, Object> parameters) throws IOException
    {
        IVersionedSerializer<T2> serializer = (IVersionedSerializer<T2>) MessagingService.verbSerializers.get(verb);
        if (serializer instanceof MessagingService.CallbackDeterminedSerializer)
        {
            CallbackInfo callback = MessagingService.instance().getRegisteredCallback(id);
            if (callback == null)
            {
                // reply for expired callback.  we'll have to skip it.
                in.skipBytesFully(payloadSize);
                return null;
            }
            serializer = (IVersionedSerializer<T2>) callback.serializer;
        }

        if (payloadSize == 0 || serializer == null)
        {
            // if there's no deserializer for the verb, skip the payload bytes to leave
            // the stream in a clean state (for the next message)
            in.skipBytesFully(payloadSize);
            return create(from, null, parameters, verb, version, constructionTime);
        }

        T2 payload = serializer.deserialize(in, version);
        return MessageIn.create(from, payload, parameters, verb, version, constructionTime);
    }

    static long deriveConstructionTime(InetAddressAndPort from, int messageTimestamp, long currentTime)
    {
        // Reconstruct the message construction time sent by the remote host (we sent only the lower 4 bytes, assuming the
        // higher 4 bytes wouldn't change between the sender and receiver)
        long sentConstructionTime = (currentTime & 0xFFFFFFFF00000000L) | (((messageTimestamp & 0xFFFFFFFFL) << 2) >> 2);

        // Because nodes may not have their clock perfectly in sync, it's actually possible the sentConstructionTime is
        // later than the currentTime (the received time). If that's the case, as we definitively know there is a lack
        // of proper synchronziation of the clock, we ignore sentConstructionTime. We also ignore that
        // sentConstructionTime if we're told to.
        long elapsed = currentTime - sentConstructionTime;
        if (elapsed > 0)
            MessagingService.instance().metrics.addTimeTaken(from, elapsed);

        boolean useSentTime = DatabaseDescriptor.hasCrossNodeTimeout() && elapsed > 0;
        return useSentTime ? sentConstructionTime : currentTime;
    }

    /**
     * Since how long (in milliseconds) the message has lived.
     */
    public long getLifetimeInMS()
    {
        return ApproximateTime.currentTimeMillis() - constructionTime;
    }

    /**
     * Whether the message has crossed the node boundary, that is whether it originated from another node.
     *
     */
    public boolean isCrossNode()
    {
        return !from.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    public Stage getMessageType()
    {
        return MessagingService.verbStages.get(verb);
    }

    public boolean doCallbackOnFailure()
    {
        return parameters.containsKey(ParameterType.FAILURE_CALLBACK);
    }

    public boolean isFailureResponse()
    {
        return parameters.containsKey(ParameterType.FAILURE_RESPONSE);
    }

    public RequestFailureReason getFailureReason()
    {
        Short code = (Short)parameters.get(ParameterType.FAILURE_REASON);
        return code != null ? RequestFailureReason.fromCode(code) : RequestFailureReason.UNKNOWN;
    }

    public long getTimeout()
    {
        return verb.getTimeout();
    }

    public long getSlowQueryTimeout()
    {
        return DatabaseDescriptor.getSlowQueryTimeout();
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("FROM:").append(from).append(" TYPE:").append(getMessageType()).append(" VERB:").append(verb);
        return sbuf.toString();
    }

    public static MessageInProcessor getProcessor(InetAddressAndPort peer, int messagingVersion)
    {
        return messagingVersion >= MessagingService.VERSION_40
               ? new MessageProcessorAsOf40(peer, messagingVersion)
               : new MessageProcessorPre40(peer, messagingVersion);

    }

    public static abstract class MessageInProcessor
    {
        // TODO:JEB document how this is only relevant to the non-blocking use cases
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
        private static final BiConsumer<MessageIn, Integer> MESSAGING_SERVICE_CONSUMER = (messageIn, id) -> MessagingService.instance().receive(messageIn, id);

        final InetAddressAndPort peer;
        final int messagingVersion;

        /**
         * Abstracts out depending directly on {@link MessagingService#receive(MessageIn, int)}; this makes tests more sane
         * as they don't require nor trigger the entire message processing circus.
         */
        final BiConsumer<MessageIn, Integer> messageConsumer;

        // TODO:JEB these fields are only used in the non-blocking case
        State state;
        MessageHeader messageHeader;

        // non-blocking
        public abstract void process(ByteBuf in) throws IOException;

        //blocking
        public abstract void process(RebufferingByteBufDataInputPlus in) throws IOException;

        MessageInProcessor(InetAddressAndPort peer, int messagingVersion)
        {
            this(peer, messagingVersion, MESSAGING_SERVICE_CONSUMER);
        }

        MessageInProcessor(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
        {
            this.peer = peer;
            this.messagingVersion = messagingVersion;
            this.messageConsumer = messageConsumer;
        }

        // should ony be used for testing!!!
        @VisibleForTesting
        MessageHeader getMessageHeader()
        {
            return messageHeader;
        }

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

        MessageHeader readFirstChunk(DataInputPlus in) throws IOException
        {
            MessagingService.validateMagic(in.readInt());
            MessageHeader messageHeader = new MessageHeader();
            messageHeader.messageId = in.readInt();
            int messageTimestamp = in.readInt(); // make sure to read the sent timestamp, even if DatabaseDescriptor.hasCrossNodeTimeout() is not enabled
            messageHeader.constructionTime = MessageIn.deriveConstructionTime(peer, messageTimestamp, ApproximateTime.currentTimeMillis());

            return messageHeader;
        }
    }

    static class MessageProcessorAsOf40 extends MessageInProcessor
    {
        MessageProcessorAsOf40(InetAddressAndPort peer, int messagingVersion)
        {
            super(peer, messagingVersion);
        }

        @SuppressWarnings("resource")
        public void process(ByteBuf in) throws IOException
        {
            ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
            while (true)
            {
                switch (state)
                {
                    case READ_FIRST_CHUNK:
                        if (in.readableBytes() < FIRST_SECTION_BYTE_COUNT)
                            return;
                        MessageHeader header = readFirstChunk(inputPlus);
                        if (header == null)
                            return;
                        header.from = peer;
                        messageHeader = header;
                        state = State.READ_VERB;
                        // fall-through
                    case READ_VERB:
                        if (in.readableBytes() < VERB_LENGTH)
                            return;
                        messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                        state = State.READ_PARAMETERS_SIZE;
                        // fall-through
                    case READ_PARAMETERS_SIZE:
                        long length = VIntCoding.readUnsignedVInt(in);
                        if (length < 0)
                            return;
                        messageHeader.parameterLength = Ints.checkedCast(length);
                        messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                        state = State.READ_PARAMETERS_DATA;
                        // fall-through
                    case READ_PARAMETERS_DATA:
                        if (messageHeader.parameterLength > 0)
                        {
                            if (in.readableBytes() < messageHeader.parameterLength)
                                return;
                            readParameters(inputPlus, messageHeader.parameterLength, messageHeader.parameters);
                        }
                        state = State.READ_PAYLOAD_SIZE;
                        // fall-through
                    case READ_PAYLOAD_SIZE:
                        length = VIntCoding.readUnsignedVInt(in);
                        if (length < 0)
                            return;
                        messageHeader.payloadSize = (int) length;
                        state = State.READ_PAYLOAD;
                        // fall-through
                    case READ_PAYLOAD:
                        if (in.readableBytes() < messageHeader.payloadSize)
                            return;

                        MessageIn<Object> messageIn = MessageIn.read(inputPlus, messagingVersion,
                                                                     messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                                     messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);

                        if (messageIn != null)
                            messageConsumer.accept(messageIn, messageHeader.messageId);

                        state = State.READ_FIRST_CHUNK;
                        messageHeader = null;
                        break;
                    default:
                        throw new IllegalStateException("unknown/unhandled state: " + state);
                }
            }
        }

        private void readParameters(DataInputPlus inputPlus, int parameterLength, Map<ParameterType, Object> parameters) throws IOException
        {
            TrackedDataInputPlus inputTracker = new TrackedDataInputPlus(inputPlus);

            while (inputTracker.getBytesRead() < parameterLength)
            {
                String key = DataInputStream.readUTF(inputPlus);
                ParameterType parameterType = ParameterType.byName.get(key);
                long valueLength = VIntCoding.readUnsignedVInt(inputTracker);
                byte[] value = new byte[Ints.checkedCast(valueLength)];
                inputTracker.readFully(value);
                try (DataInputBuffer buffer = new DataInputBuffer(value))
                {
                    parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
                }
            }
        }

        public void process(RebufferingByteBufDataInputPlus in) throws IOException
        {
            while (in.isOpen())
            {
                MessageHeader header = readFirstChunk(in);
                header.from = peer;
                messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                messageHeader.parameterLength = Ints.checkedCast(VIntCoding.readUnsignedVInt(in));
                messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                if (messageHeader.parameterLength > 0)
                    readParameters(in, messageHeader.parameterLength, messageHeader.parameters);

                messageHeader.payloadSize = Ints.checkedCast(VIntCoding.readUnsignedVInt(in));
                MessageIn<Object> messageIn = MessageIn.read(in, messagingVersion,
                                                             messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                             messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);
                if (messageIn != null)
                    messageConsumer.accept(messageIn, messageHeader.messageId);
            }
        }
    }

    static class MessageProcessorPre40 extends MessageInProcessor
    {
        private static final int PARAMETERS_SIZE_LENGTH = Integer.BYTES;
        private static final int PARAMETERS_VALUE_SIZE_LENGTH = Integer.BYTES;
        private static final int PAYLOAD_SIZE_LENGTH = Integer.BYTES;

        MessageProcessorPre40(InetAddressAndPort peer, int messagingVersion)
        {
            super(peer, messagingVersion);
        }

        public void process(ByteBuf in) throws IOException
        {
            ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
            while (true)
            {
                switch (state)
                {
                    case READ_FIRST_CHUNK:
                        if (in.readableBytes() < FIRST_SECTION_BYTE_COUNT)
                            return;
                        MessageHeader header = readFirstChunk(inputPlus);
                        if (header == null)
                            return;
                        messageHeader = header;
                        state = State.READ_IP_ADDRESS;
                        // fall-through
                    case READ_IP_ADDRESS:
                        // unfortunately, this assumes knowledge of how CompactEndpointSerializationHelper serializes data (the first byte is the size).
                        // first, check that we can actually read the size byte, then check if we can read that number of bytes.
                        // the "+ 1" is to make sure we have the size byte in addition to the serialized IP addr count of bytes in the buffer.
                        int readableBytes = in.readableBytes();
                        if (readableBytes < 1 || readableBytes < in.getByte(in.readerIndex()) + 1)
                            return;
                        messageHeader.from = CompactEndpointSerializationHelper.instance.deserialize(inputPlus, messagingVersion);
                        state = State.READ_VERB;
                        // fall-through
                    case READ_VERB:
                        if (in.readableBytes() < VERB_LENGTH)
                            return;
                        messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                        state = State.READ_PARAMETERS_SIZE;
                        // fall-through
                    case READ_PARAMETERS_SIZE:
                        if (in.readableBytes() < PARAMETERS_SIZE_LENGTH)
                            return;
                        messageHeader.parameterLength = in.readInt();
                        messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                        state = State.READ_PARAMETERS_DATA;
                        // fall-through
                    case READ_PARAMETERS_DATA:
                        if (messageHeader.parameterLength > 0)
                        {
                            if (!readParameters(in, inputPlus, messageHeader.parameterLength, messageHeader.parameters))
                                return;
                        }
                        state = State.READ_PAYLOAD_SIZE;
                        // fall-through
                    case READ_PAYLOAD_SIZE:
                        if (in.readableBytes() < PAYLOAD_SIZE_LENGTH)
                            return;
                        messageHeader.payloadSize = in.readInt();
                        state = State.READ_PAYLOAD;
                        // fall-through
                    case READ_PAYLOAD:
                        if (in.readableBytes() < messageHeader.payloadSize)
                            return;

                        MessageIn<Object> messageIn = MessageIn.read(inputPlus, messagingVersion,
                                                                     messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                                     messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);

                        if (messageIn != null)
                            messageConsumer.accept(messageIn, messageHeader.messageId);

                        state = State.READ_FIRST_CHUNK;
                        messageHeader = null;
                        break;
                    default:
                        throw new IllegalStateException("unknown/unhandled state: " + state);
                }
            }
        }

        /**
         * @return <code>true</code> if all the parameters have been read from the {@link ByteBuf}; else, <code>false</code>.
         */
        private boolean readParameters(ByteBuf in, ByteBufDataInputPlus inputPlus, int parameterCount, Map<ParameterType, Object> parameters) throws IOException
        {
            // makes the assumption that map.size() is a constant time function (HashMap.size() is)
            while (parameters.size() < parameterCount)
            {
                if (!canReadNextParam(in))
                    return false;

                String key = DataInputStream.readUTF(inputPlus);
                ParameterType parameterType = ParameterType.byName.get(key);
                byte[] value = new byte[in.readInt()];
                in.readBytes(value);
                try (DataInputBuffer buffer = new DataInputBuffer(value))
                {
                    parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
                }
            }

            return true;
        }

        /**
         * Determine if we can read the next parameter from the {@link ByteBuf}. This method will *always* set the {@code in}
         * readIndex back to where it was when this method was invoked.
         * <p>
         * NOTE: this function would be sooo much simpler if we included a parameters length int in the messaging format,
         * instead of checking the remaining readable bytes for each field as we're parsing it. c'est la vie ...
         */
        @VisibleForTesting
        boolean canReadNextParam(ByteBuf in)
        {
            in.markReaderIndex();
            // capture the readableBytes value here to avoid all the virtual function calls.
            // subtract 6 as we know we'll be reading a short and an int (for the utf and value lengths).
            final int minimumBytesRequired = 6;
            int readableBytes = in.readableBytes() - minimumBytesRequired;
            if (readableBytes < 0)
                return false;

            // this is a tad invasive, but since we know the UTF string is prefaced with a 2-byte length,
            // read that to make sure we have enough bytes to read the string itself.
            short strLen = in.readShort();
            // check if we can read that many bytes for the UTF
            if (strLen > readableBytes)
            {
                in.resetReaderIndex();
                return false;
            }
            in.skipBytes(strLen);
            readableBytes -= strLen;

            // check if we can read the value length
            if (readableBytes < PARAMETERS_VALUE_SIZE_LENGTH)
            {
                in.resetReaderIndex();
                return false;
            }
            int valueLength = in.readInt();
            // check if we read that many bytes for the value
            if (valueLength > readableBytes)
            {
                in.resetReaderIndex();
                return false;
            }

            in.resetReaderIndex();
            return true;
        }

        public void process(RebufferingByteBufDataInputPlus in) throws IOException
        {
            while (in.isOpen())
            {
                MessageHeader header = readFirstChunk(in);
                header.from = peer;
                messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                messageHeader.parameterLength = in.readInt();
                messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                if (messageHeader.parameterLength > 0)
                    readParameters(in, messageHeader.parameterLength, messageHeader.parameters);

                messageHeader.payloadSize = in.readInt();
                MessageIn<Object> messageIn = MessageIn.read(in, messagingVersion,
                                                             messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                             messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);
                if (messageIn != null)
                    messageConsumer.accept(messageIn, messageHeader.messageId);
            }
        }

        private void readParameters(RebufferingByteBufDataInputPlus in, int parameterCount, Map<ParameterType, Object> parameters) throws IOException
        {
            // makes the assumption that map.size() is a constant time function (HashMap.size() is)
            while (parameters.size() < parameterCount)
            {
                String key = DataInputStream.readUTF(in);
                ParameterType parameterType = ParameterType.byName.get(key);
                byte[] value = new byte[in.readInt()];
                in.read(value);
                try (DataInputBuffer buffer = new DataInputBuffer(value))
                {
                    parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
                }
            }
        }
    }
}
