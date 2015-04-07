package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.peersampling.messages.HyParViewMessage;

public class SerialHyParViewService extends HyParViewService
{
    public SerialHyParViewService(HPVConfig config, GossipDispatcher dispatcher)
    {
        super(config, dispatcher);
    }

    public synchronized void handle(HyParViewMessage msg, InetAddress sender)
    {
        super.handle(msg, sender);
    }
}
