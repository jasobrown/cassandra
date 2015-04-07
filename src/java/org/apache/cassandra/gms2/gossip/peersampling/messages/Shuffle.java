package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class Shuffle implements HyParViewMessage
{
    private static final ISerializer<Shuffle> serializer = new Serializer();

    private final InetAddress originator;
    private final Collection<InetAddress> nodes;
    private final int timeToLive;

    public Shuffle(InetAddress originator, Collection<InetAddress> peers, int timeToLive)
    {
        this.originator = originator;
        this.nodes = peers;
        this.timeToLive = timeToLive;
    }

    public Collection<InetAddress> getNodes()
    {
        return nodes;
    }

    public InetAddress getOriginator()
    {
        return originator;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    /**
     * Shallow clone and decrement the {@code timeToLive} value.
     */
    public Shuffle cloneForForwarding()
    {
        return new Shuffle(originator, nodes, timeToLive - 1);
    }

    public MessageType getMessageType()
    {
        return MessageType.SHUFFLE;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<Shuffle>
    {
        public Shuffle deserialize(DataInput in) throws IOException
        {
            return null;
        }

        public void serialize(Shuffle msg, DataOutputPlus out) throws IOException
        {

        }

        public long serializedSize(Shuffle msg, TypeSizes type)
        {
            return 0;
        }
    }
}
