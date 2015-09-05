package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;
import org.apache.cassandra.gossip.hyparview.NeighborRequestMessage.Priority;
import org.apache.cassandra.gossip.hyparview.NeighborResponseMessage.Result;

/**
 * An implementation of the HyParView paper, Leitao, et al, 2008
 * TODO: link to paper!!!!
 *
 * However, we some liberties with our implementation. HyParView requires two views of peers
 * in the cluster: an active view and a passive view. We maintain the active in this class,
 * but we can piggy-back off the existing knowledge of the full cluster as a placement
 * for all the mechanics of maintaining the passive view (see 4.4 of the paper, especially
 * the part on the SHUFFLE messages). We still fulfill the needs of the passive view (having a backlog
 * of peers to use as a backup), but save ourselves some code/network traffic/extra functionality.
 *
 * As for the active view, we split it into two parts: a view of active peers for the local datacenter,
 * and a map which will hold one active peer for each remote datacenter. This way, we can keep
 * at least one node from every datacenter in the "aggregate active view". Note: the HyParView paper
 * does not deal with network partitions (it was solving other problems), and so this is our way of handling
 * the need to keep all DCs in the active view.
 *
 * Note: this class is *NOT* thread-safe, and intentionally so, in order to keep it simple and efficient.
 */
public class HyParView implements PeerSamplingService, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(HyParView.class);
    private static final long DEFAULT_RANDOM_SEED = "BuyMyDatabass".hashCode();

    private final Set<PeerSamplingServiceListener> listeners;
    private final EndpointStateSubscriber endpointStateSubscriber;

    /**
     * Active view of peers in the local datacenter. The max size of this collection
     * should hover around the max fanout value (based on number of nodes in the datacenter).
     */
    private final LinkedList<InetAddress> localDatacenterView;

    /**
     * Mapping of a datacenter name to a single peer in that datacenter.
     */
    private Map<String, InetAddress> remoteView;

    /**
     * The braodcast address of the local node. We memoize it here to avoid a hard dependency on FBUtilities.
     */
    private final InetAddress localAddress;

    /**
     * Cassandra datacenter this node is executing in.
     */
    private final String datacenter;

    private final HyParViewConfiguration config;

    /**
     * A fixed local random number generator, mainly to provide consistency in testing.
     */
    private final Random random;

    private MessageSender messageSender;

    public HyParView(InetAddress localAddress, String datacenter, HyParViewConfiguration config)
    {
        this.localAddress = localAddress;
        this.datacenter = datacenter;
        this.config = config;
        random = new Random(DEFAULT_RANDOM_SEED);
        listeners = new HashSet<>();
        endpointStateSubscriber = new EndpointStateSubscriber();

        localDatacenterView = new LinkedList<>();
        remoteView = new HashMap<>();
    }

    public void init(MessageSender messageSender)
    {
        this.messageSender = messageSender;

        // TODO:JEB fish out the initial Endpoint states

        Gossiper.instance.register(endpointStateSubscriber);

        join();
    }

    /**
     * Sends out a message to a randomly selected seed node.
     */
    void join()
    {
        List<InetAddress> seeds = new ArrayList<>(config.seedProvider().getSeeds());
        seeds.remove(localAddress);
        if (seeds.isEmpty())
        {
            logger.info("no seeds left in the seed list (after removing this node), so will wait for other nodes to join to start gossiping");
            return;
        }

        Collections.shuffle(seeds, random);

        // TODO: add a callback mechanism to ensure we've received some response from
        // the rest of the cluster - else, we need to try to join again.
        messageSender.send(seeds.get(0), new JoinMessage(localAddress, datacenter));
    }

    public void receiveMessage(HyParViewMessage message)
    {
        switch (message.getMessageType())
        {
            case JOIN: handleJoin((JoinMessage)message); break;
            case JOIN_RESPONSE: handleJoinResponse((JoinResponseMessage)message); break;
            case FORWARD_JOIN: handleForwardJoin((ForwardJoinMessage)message); break;
            case NEIGHBOR_REQUEST: handleNeighborRequest((NeighborRequestMessage)message); break;
            case NEIGHBOR_RESPONSE: handleNeighborResponse((NeighborResponseMessage)message); break;
            case DISCONNECT: handleDisconnect((DisconnectMessage)message); break;
            default:
                throw new IllegalArgumentException("Unhandled hyparview message type: " + message.getMessageType());
        }
    }

    /**
     * Handle an incoming request message to JOIN the HyParView subsystem. When we receive a join,
     * we add that node to our passive view (not disturbing any upstream dependants, though), maybe
     * disconnecting from a node currently in the view (if we're at the max size limit).
     * Then we send out a FORWARD_JOIN message to all peers in the active view.
     * 
     * Note: except for tests, there should be no direct outside callers.
     */
    @VisibleForTesting
    void handleJoin(JoinMessage message)
    {
        // this might be a re-broadcast from the sender (because it never got a
        // response to it's JOIN request), so always process (and don't ignore)
        // notify listeners

        addToView(message.requestor, message.datacenter);

        messageSender.send(message.requestor, new JoinResponseMessage(message.requestor, message.datacenter));

        peerUp(message.requestor, message.datacenter);

        // determine the forwarding join targets before we start mucking with the views
        Collection<InetAddress> activeView = getPeers();
        activeView.remove(message.requestor);

        if (!activeView.isEmpty())
        {
            ForwardJoinMessage msg = new ForwardJoinMessage(message.requestor, message.datacenter, localAddress, config.activeRandomWalkLength());
            for (InetAddress activePeer : activeView)
                messageSender.send(activePeer, msg);
        }
        else
        {
            logger.debug("no other nodes available to send a forward join message to");
        }
    }

    /**
     * Find a random target node in the requested datacenter.
     */
    InetAddress findArbitraryTarget(Optional<InetAddress> sender, String datacenter)
    {
        List<InetAddress> candidates;
        if (this.datacenter.equals(datacenter))
        {
            candidates = new ArrayList<>(localDatacenterView);
        }
        else
        {
            // first check if we have a peer in the active view for the remote datacenter
            InetAddress remotePeer = remoteView.get(datacenter);

            if (remotePeer == null || (sender.isPresent() && remotePeer.equals(sender.get())))
            {
                // if there's no active remote peer, try the entire list of remote peers,
                // and failing that, use local datacenter.
                Collection<InetAddress> allRemotes = endpointStateSubscriber.peers.get(datacenter);
                candidates = new ArrayList<>(allRemotes != null ? allRemotes : localDatacenterView);
            }
            else
                candidates = new ArrayList<InetAddress>() {{ add(remotePeer); }};
        }

        candidates.remove(localAddress);
        if (sender.isPresent())
            candidates.remove(sender);

        if (candidates.isEmpty())
            return null;
        if (candidates.size() == 1)
            return candidates.get(0);
        Collections.shuffle(candidates, random);
        return candidates.get(0);
    }

    void addToView(InetAddress peer, String datacenter)
    {
        if (this.datacenter.equalsIgnoreCase(datacenter))
            addToLocalActiveView(peer);
        else
            addToRemoteActiveView(peer, datacenter);
    }

    /**
     * Add peer to the local active view.
     */
    @VisibleForTesting
    void addToLocalActiveView(InetAddress peer)
    {
        if (!localDatacenterView.contains(peer))
        {
            localDatacenterView.addLast(peer);
            if (localDatacenterView.size() > endpointStateSubscriber.fanout(datacenter, datacenter))
                removeNode(localDatacenterView.removeFirst(), datacenter);
        }
    }

    void removeNode(InetAddress peer, String datacenter)
    {
        messageSender.send(peer, new DisconnectMessage(peer, datacenter));

        // TODO: do we need to contact listeners????? don't think so, but ....
    }

    void addToRemoteActiveView(InetAddress peer, String datacenter)
    {
        InetAddress previous = remoteView.put(datacenter, peer);
        if (previous == null || previous.equals(peer))
            return;
        removeNode(previous, datacenter);
    }

    void peerUp(InetAddress addr, String datacenter)
    {
        for (PeerSamplingServiceListener listener : listeners)
            listener.neighborUp(addr, datacenter);
    }

    @VisibleForTesting
    void handleJoinResponse(JoinResponseMessage message)
    {
        // TODO: make sure we really need this, or just a regular c* RR
    }

    /**
     * Handle an incoming forward join message. If the message's time-to-live is greater than 0,
     * forward the message to a node from the active view (avoiding sending back to the peer that forwarded
     * to us). If the message's time-to-live is 0, or we have <= 1 nodes in local DC's view (or remote datacenter view is empty),
     * add to local active view and respond back to requesting node.
     *
     * Note: except for tests, there should be no direct outside callers.
     */
    @VisibleForTesting
    void handleForwardJoin(ForwardJoinMessage message)
    {
        int nextTTL = message.timeToLive - 1;
        boolean added = false;
        if (message.datacenter.equals(datacenter))
        {
            if (nextTTL == 0 || localDatacenterView.size() <= 1)
            {
                addToLocalActiveView(message.requestor);
                added = true;
            }
        }
        else
        {
            if (nextTTL == 0 || !remoteView.containsKey(message.datacenter))
            {
                addToRemoteActiveView(message.requestor, message.datacenter);
                added = true;
            }
        }

        if (added)
        {
            messageSender.send(message.requestor, new JoinResponseMessage(localAddress, datacenter));
        }
        else
        {
            InetAddress peer = findArbitraryTarget(Optional.of(message.requestor), message.datacenter);
            messageSender.send(peer, new ForwardJoinMessage(message.requestor, message.datacenter, localAddress, nextTTL));
        }
    }

    /**
     * Handle a neighbor connection request. If the message has a high priority, we must accept it.
     * If a low priority, check if we have space in the active view (for the peer's datacenter), and accept
     * the connect is there an open slot.
     *
     * Note: except for tests, there should be no direct outside callers.
     */
    @VisibleForTesting
    void handleNeighborRequest(NeighborRequestMessage message)
    {
        if (message.priority == Priority.LOW)
        {
            if ((message.datacenter.equals(datacenter) && endpointStateSubscriber.fanout(datacenter, datacenter) >= localDatacenterView.size())
                 || remoteView.containsKey(message.datacenter))
            {
                messageSender.send(message.requestor, new NeighborResponseMessage(localAddress, datacenter, Result.DENY));
                return;
            }
        }

        addToView(message.requestor, message.datacenter);
        messageSender.send(message.requestor, new NeighborResponseMessage(localAddress, datacenter, Result.ACCEPT));
    }

    /**
     * Note: except for tests, there should be no direct outside callers.
     */
    @VisibleForTesting
    void handleNeighborResponse(NeighborResponseMessage message)
    {
        if (message.result == Result.ACCEPT)
            addToView(message.requestor, message.datacenter);
        else
            sendNeighborRequest(Optional.of(message.requestor), message.datacenter);
    }

    /**
     * Remove the requestor from our active view, and, if it was actaully in our active view, try
     * to replace it with a node from the passive view.
     *
     * Note: except for tests, there should be no direct outside callers.
     */
    @VisibleForTesting
    void handleDisconnect(DisconnectMessage message)
    {
        if (localDatacenterView.remove(message.requestor) || remoteView.remove(message.datacenter, message.requestor))
            sendNeighborRequest(Optional.of(message.requestor), message.datacenter);
    }

    void sendNeighborRequest(Optional<InetAddress> filtered, String datacenter)
    {
        // remove node from active view
        List<InetAddress> candidates;
        if (this.datacenter.equals(datacenter))
        {
            candidates = new ArrayList<>(endpointStateSubscriber.peers.get(datacenter));
            // filter out nodes already in the active view
            candidates.removeAll(localDatacenterView);
            candidates.remove(localAddress);
        }
        else
        {
            Collection<InetAddress> remotePeers = endpointStateSubscriber.peers.get(datacenter);
            if (remotePeers == null || remotePeers.isEmpty())
            {
                logger.debug("no more peers from remote datacenter " + datacenter);
                return;
            }
            candidates = new ArrayList<>(remotePeers);
        }

        if (filtered.isPresent())
            candidates.remove(filtered);
        if (candidates.isEmpty())
            return;
        Collections.shuffle(candidates, random);
        // TODO: handle case where local DC is empty, as we need to send high priority (preferrably to own DC)
        messageSender.send(candidates.get(0), new NeighborRequestMessage(localAddress, datacenter, Priority.LOW));
    }

    public Collection<InetAddress> getPeers()
    {
        List<InetAddress> peers = new ArrayList<>(localDatacenterView.size() + remoteView.size());
        peers.addAll(localDatacenterView);
        peers.addAll(remoteView.values());

        return peers;
    }

    public void register(PeerSamplingServiceListener listener)
    {
        listeners.add(listener);
    }

    public void unregister(PeerSamplingServiceListener listener)
    {
        listeners.remove(listener);
    }

    void peerUnavailable(InetAddress addr)
    {
        Optional<String> datacenter = endpointStateSubscriber.getDatacenter(addr);
        if (datacenter.isPresent())
            peerUnavailable(addr, datacenter.get());
    }

    /**
     * Utility for informing all listeners that a perr in the cluster is either unavailable or has been explicitly
     * marked down.
     *
     * @param addr The address of the unavailable peer.
     */
    void peerUnavailable(InetAddress addr, String datacenter)
    {
        for (PeerSamplingServiceListener listener : listeners)
            listener.neighborDown(addr, datacenter);
        sendNeighborRequest(Optional.of(addr), datacenter);
    }

    class EndpointStateSubscriber implements IEndpointStateChangeSubscriber
    {
        /**
         * The minimum size for the number of nodes in a local datacenter to be larger than
         * to use the natural log for the fanout value.
         */
        private static final int NATURAL_LOG_THRESHOLD = 16;

        /**
         * Collection of statuses that we consider a peer down or unavailable for.
         */
        private final List<String> DOWN_STATUSES = new ArrayList<String>()
        {{
            add(VersionedValue.SHUTDOWN);
            add(VersionedValue.REMOVING_TOKEN);
            add(VersionedValue.REMOVED_TOKEN);
            add(VersionedValue.STATUS_LEAVING);
            add(VersionedValue.STATUS_LEFT);
        }};

        /**
         * Internal mapping of all nodes to their respective datacenters.
         *
         * as current Gossiper is single-threaded, we can *probably* use the multimap
         * with a decent degree of certainty we'll be thread-safe
         */
        private final Multimap<String, InetAddress> peers;

        EndpointStateSubscriber()
        {
            peers = HashMultimap.create();
        }

        /**
         * Determine the fanout (number of nodes to contact) for the target datacenter.
         * If the target is different from the local datacenter, always returns 1. If it is
         * the same datacenter, and if the number of nodes if below some threshold, return
         * the number of nodes in the datacenter (to contact them all); else, if, there's a
         * large number of nodes in the local datacenter, return the natural log of the cluster.
         */
        public int fanout(String localDatacenter, String targetDatacenter)
        {
            if (!localDatacenter.equals(targetDatacenter))
                return 1;

            Collection<InetAddress> localPeers = peers.get(localDatacenter);
            // if there are no peers, really doesn't matter so much what the fanout size is ...
            if (localPeers == null)
                return NATURAL_LOG_THRESHOLD;

            int localPeerCount = localPeers.size();
            if (localPeerCount >= NATURAL_LOG_THRESHOLD)
                return (int)Math.ceil(Math.log(localPeerCount));
            return localPeerCount;
        }

        public Optional<String> getDatacenter(InetAddress addr)
        {
            // hoping there's a more efficient way of getting the datacenter for a node...
            for (Map.Entry<String, Collection<InetAddress>> entry : peers.asMap().entrySet())
            {
                if (entry.getValue().contains(addr))
                    return Optional.of(entry.getKey());
            }
            return Optional.empty();
        }

        public void onJoin(InetAddress endpoint, EndpointState epState)
        {

        }

        public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
        {
            // nop
        }

        public void onChange(InetAddress addr, ApplicationState state, VersionedValue value)
        {
//            if (state != ApplicationState.STATUS)
//                return;
//            if (DOWN_STATUSES.contains(value))
//                peerUnavailable(addr);
        }

        public void onAlive(InetAddress endpoint, EndpointState state)
        {
            // nop
        }

        public void onDead(InetAddress endpoint, EndpointState state)
        {
            peerUnavailable(endpoint);
        }

        public void onRemove(InetAddress endpoint)
        {
            peerUnavailable(endpoint);
        }

        public void onRestart(InetAddress endpoint, EndpointState state)
        {

        }
    }

    /*
        methods for IFailureDetectionEventListener
     */
    public void convict(InetAddress addr, double phi)
    {
        // TODO: not entirely clear if we should listen to the whims of the FD, but go with it for now
        peerUnavailable(addr);
    }
}
