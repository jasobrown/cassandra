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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.utils.vint.VIntCoding;

public abstract class MessageProcessor
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
    private static final int FIRST_SECTION_BYTE_COUNT = 12;

    static final int VERB_LENGTH = Integer.BYTES;

    /**
     * The default target for consuming deserialized {@link MessageIn}.
     */
    static final BiConsumer<MessageIn, Integer> MESSAGING_SERVICE_CONSUMER = (messageIn, id) -> MessagingService.instance().receive(messageIn, id);

    final InetAddressAndPort peer;
    final int messagingVersion;

    /**
     * Abstracts out depending directly on {@link MessagingService#receive(MessageIn, int)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    final BiConsumer<MessageIn, Integer> messageConsumer;

    // TODO:JEB these fields are only used in the non-blocking case
    private State state;
    private MessageHeader messageHeader;

    // non-blocking
    abstract void process(ByteBuf in) throws IOException;
    //blocking
    abstract void process(RebufferingByteBufDataInputPlus in) throws IOException;

    MessageProcessor(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        this.peer = peer;
        this.messagingVersion = messagingVersion;
        this.messageConsumer = messageConsumer;
    }

    public static MessageProcessor create(InetAddressAndPort peer, int messagingVersion)
    {
        return create(peer, messagingVersion, MESSAGING_SERVICE_CONSUMER);
    }

    public static MessageProcessor create(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        return messagingVersion >= MessagingService.VERSION_40
               ? new MessageProcessorAsOf40(peer, messagingVersion, messageConsumer)
               : new MessageProcessorPre40(peer, messagingVersion, messageConsumer);
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


    class MessageProcessorAsOf40 extends MessageProcessor
    {
        MessageProcessorAsOf40(InetAddressAndPort peer, int messagingVersion)
        {
            super(peer, messagingVersion);
        }

        @SuppressWarnings("resource")
        void process(ByteBuf in) throws IOException
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
                            readParameters(in, inputPlus, messageHeader.parameterLength, messageHeader.parameters);
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

                        // TODO consider deserailizing the messge not on the event loop
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

        private void readParameters(ByteBuf in, ByteBufDataInputPlus inputPlus, int parameterLength, Map<ParameterType, Object> parameters) throws IOException
        {
            // makes the assumption we have all the bytes required to read the headers
            final int endIndex = in.readerIndex() + parameterLength;
            while (in.readerIndex() < endIndex)
            {
                String key = DataInputStream.readUTF(inputPlus);
                ParameterType parameterType = ParameterType.byName.get(key);
                long valueLength = VIntCoding.readUnsignedVInt(in);
                byte[] value = new byte[Ints.checkedCast(valueLength)];
                in.readBytes(value);
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
                    readParameters(??, in, messageHeader.parameterLength, messageHeader.parameters);

                messageHeader.payloadSize = Ints.checkedCast(VIntCoding.readUnsignedVInt(in));
                MessageIn<Object> messageIn = MessageIn.read(in, messagingVersion,
                                                             messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                             messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);
                if (messageIn != null)
                    messageConsumer.accept(messageIn, messageHeader.messageId);
            }
        }
    }

    private class MessageProcessorPre40 extends MessageProcessor
    {
        static final int PARAMETERS_SIZE_LENGTH = Integer.BYTES;
        static final int PARAMETERS_VALUE_SIZE_LENGTH = Integer.BYTES;
        static final int PAYLOAD_SIZE_LENGTH = Integer.BYTES;

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

                        // TODO consider deserailizing the messge not on the event loop
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

        private boolean readParameters(RebufferingByteBufDataInputPlus in, int parameterCount, Map<ParameterType, Object> parameters) throws IOException
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

            return true;
        }
    }
}
