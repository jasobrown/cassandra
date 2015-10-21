package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public class PruneMessage extends ThicketMessage
{
    public final Collection<InetAddress> treeRoots;
    public final Collection<LoadEstimate> estimates;

    public PruneMessage(InetAddress sender, GossipMessageId messageId, Collection<InetAddress> treeRoots, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId);
        this.treeRoots = treeRoots;
        this.estimates = estimates;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.PRUNE;
    }
}
