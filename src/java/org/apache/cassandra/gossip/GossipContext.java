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
package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gossip.hyparview.HyParViewMessageSender;
import org.apache.cassandra.gossip.hyparview.HyParViewService;
import org.apache.cassandra.gossip.thicket.ThicketMessageSender;
import org.apache.cassandra.gossip.thicket.ThicketService;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class GossipContext
{
    /**
     * Until entire new gossip stack is ready, only start up when explicitly requested.
     */
    private final boolean enabled;

    public final HyParViewService hyparviewService;
    public final ThicketService thicketService;
    private final GossipStateChangeListener gossipListener;

    public GossipContext()
    {
        enabled = DatabaseDescriptor.newGossipEnabled();
        InetAddress localAddress = FBUtilities.getBroadcastAddress();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        String datacenter = snitch.getDatacenter(localAddress);

        hyparviewService = new HyParViewService(localAddress, datacenter, DatabaseDescriptor.getSeedProvider(), new HyParViewMessageSender(),
                                               StageManager.getStage(Stage.GOSSIP), ScheduledExecutors.scheduledTasks);
        thicketService = new ThicketService(localAddress, new ThicketMessageSender(),
                                            StageManager.getStage(Stage.GOSSIP), ScheduledExecutors.scheduledTasks);
        hyparviewService.register(thicketService);
        gossipListener = new GossipStateChangeListener(localAddress, thicketService);
        thicketService.register(gossipListener);

        if (enabled)
        {
            Gossiper.instance.register(hyparviewService.endpointStateSubscriber);
            Gossiper.instance.register(gossipListener);
        }
    }

    public void start(int epoch)
    {
        if (!enabled)
            return;
        hyparviewService.start(epoch);
        thicketService.start(hyparviewService, epoch);
    }

    public void shutdown()
    {
        if (!enabled)
            return;
        hyparviewService.shutdown();
        thicketService.shutdown();
    }
}
