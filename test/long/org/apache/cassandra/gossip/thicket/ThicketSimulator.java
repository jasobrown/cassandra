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
package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gossip.hyparview.HyParViewSimulator;
import org.apache.cassandra.utils.Pair;

public class ThicketSimulator
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketSimulator.class);
    TimesSquareDispacher dispatcher;
    AtomicInteger messagePayload;
    Multimap<InetAddress, Object> sentMessages;

    @Before
    public void setup()
    {
        messagePayload = new AtomicInteger();
        sentMessages = HashMultimap.create();
    }

    @After
    public void tearDown()
    {
        if (dispatcher != null)
            dispatcher.shutdown();
    }

    @Test
    public void basicRun() throws UnknownHostException
    {
        System.out.println("***** thicket simulation - establish peer sampling service *****");
        dispatcher = new TimesSquareDispacher(true);
        HyParViewSimulator.executeCluster(dispatcher, new int[] { 9 });
        HyParViewSimulator.assertCluster(dispatcher);
        dispatcher.dumpCurrentState();

        System.out.println("***** thicket simulation - broadcasting messages *****");
//        for (int i = 0; i < 1; i++)
//            broadcastMessages(dispatcher, 1);
        TimesSquareDispacher.BroadcastNodeContext cxt = dispatcher.selectRandom();
        for (int i = 0; i < 10; i++)
        {
            broadcastMessage(cxt);
            if (i % 10 == 0)
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
        dispatcher.awaitQuiesence();

        // give the broadcast trees time to GRAFT, PRUNE, and so on
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        System.out.println("***** thicket simulation - assert trees *****");
        assertBroadcastTree(dispatcher, sentMessages);

        System.out.println("thicket simulation - complete!");
    }

    private void broadcastMessages(TimesSquareDispacher dispatcher, int msgCount)
    {
        for (int i = 0; i < msgCount; i++)
            broadcastMessage(dispatcher.selectRandom());

        dispatcher.awaitQuiesence();

        // give the broadcast trees time to GRAFT, PRUNE, and so on
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }

    private void broadcastMessage(TimesSquareDispacher.BroadcastNodeContext ctx)
    {
        String payload = String.valueOf(messagePayload.getAndIncrement());
        ctx.thicketService.broadcast(payload, ctx.client);
        sentMessages.put(ctx.thicketService.getLocalAddress(), payload);
    }

    private static void assertBroadcastTree(TimesSquareDispacher dispatcher, Multimap<InetAddress, Object> sentMessages)
    {
        stopThickets(dispatcher);

        for (InetAddress addr : dispatcher.getPeers())
        {
            TimesSquareDispacher.BroadcastNodeContext ctx = dispatcher.getContext(addr);
            assertFullCoverage(ctx.thicketService, dispatcher);
//            assertAllMessagesReceived(ctx, sentMessages);
        }
    }

    private static void assertFullCoverage(ThicketService thicket, TimesSquareDispacher dispatcher)
    {
        InetAddress treeRoot = thicket.getLocalAddress();

        // only bother exectuing this check if the ever sent a message (and it was a tree-root)
        if (thicket.getBroadcastedMessageCount() == 0)
            return;

        // for each tree-root, ensure full tree coverage
        Set<InetAddress> completePeers = new HashSet<>(dispatcher.getPeers());

        // put into the queue each child branch Thicket instance plus it's parent
        Queue<Pair<ThicketService, InetAddress>>queue = new LinkedList<>();
        queue.add(Pair.create(thicket, null));

        while (!queue.isEmpty())
        {
            Pair<ThicketService, InetAddress> pair = queue.poll();
            ThicketService svc = pair.left;
            completePeers.remove(svc.getLocalAddress());

            ThicketService.BroadcastPeers broadcastPeers = svc.getBroadcastPeers().get(treeRoot);
            if (broadcastPeers == null)
            {
                logger.error(String.format("%s does not have any peers for treeRoot %s, parent = %s", svc.getLocalAddress(), treeRoot, pair.right));
                continue;
            }

            // check if the instance (in the tree) has an active reference to the parent
            if (pair.right != null && !broadcastPeers.active.contains(pair.right))
                 logger.error(String.format("%s does not have a reference to it's parent (%s) in it's active peers", svc.getLocalAddress(), pair.right));

            // bail out if we have a fully connected mesh
            if (completePeers.isEmpty())
                return;

            for (InetAddress peer : broadcastPeers.active)
            {
                ThicketService branch = dispatcher.getContext(peer).thicketService;

                if (completePeers.contains(branch.getLocalAddress()))
                    queue.add(Pair.create(branch, svc.getLocalAddress()));
            }
        }

        // TODO:JEB someday make this an assert that fails - using for information now
        logger.info(String.format("*** %s broadcast peers: %s", treeRoot, thicket.getBroadcastPeers()));
        logger.error(String.format("%s cannot reach the following peers %s", thicket.getLocalAddress(), completePeers));
    }

    private static String toCsv(InetAddress localAddress, Set<InetAddress> peers)
    {
        if (peers.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append(localAddress);
        for (InetAddress peer : peers)
            sb.append(',').append(peer);
        sb.append("\n");
        return sb.toString();
    }

    private static void assertAllMessagesReceived(TimesSquareDispacher.BroadcastNodeContext ctx, Multimap<InetAddress, Object> sentMessages)
    {
        int receivedSize = ((TimesSquareDispacher.SimpleClient) ctx.client).received.size();
        if (receivedSize + ctx.thicketService.getBroadcastedMessageCount() != sentMessages.size())
        {
            logger.error(String.format("%s only recevied %d messages, out of a toal of %d",
                                       ctx.thicketService.getLocalAddress(), receivedSize  + ctx.thicketService.getBroadcastedMessageCount(), sentMessages.size()));
            return;
        }

        for (Map.Entry<InetAddress, Collection<Object>> entry : sentMessages.asMap().entrySet())
        {
            for (Object payload : entry.getValue())
            {
                if (entry.getKey().equals(ctx.thicketService.getLocalAddress()))
                    continue;
                if (ctx.client.receive(payload.toString()))
                {
                    logger.info(String.format("%s is missing from tree-root %s value %s",
                                              ctx.thicketService.getLocalAddress(), entry.getKey(), payload));
//                  Assert.assertFalse();
                }
            }
        }
    }

    private static void stopThickets(TimesSquareDispacher dispatcher)
    {
        for (InetAddress addr : dispatcher.getPeers())
            dispatcher.getContext(addr).thicketService.pause();
    }
}
