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

package org.apache.cassandra.transport;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

@RunWith(OrderedJUnit4ClassRunner.class)
public class InflightRequestPayloadTrackerTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".atable");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }

    @Test
    public void testQueryExecutionWithOverloadedExceptionEnabled() throws Throwable
    {
        SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(),
                                               nativePort,
                                               ProtocolVersion.V5,
                                               true,
                                               new EncryptionOptions());

        try
        {
            client.connect(false, false, true);
            QueryOptions queryOptions = QueryOptions.create(
            QueryOptions.DEFAULT.getConsistency(),
            QueryOptions.DEFAULT.getValues(),
            QueryOptions.DEFAULT.skipMetadata(),
            QueryOptions.DEFAULT.getPageSize(),
            QueryOptions.DEFAULT.getPagingState(),
            QueryOptions.DEFAULT.getSerialConsistency(),
            ProtocolVersion.V5,
            KEYSPACE);

            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         queryOptions);
            client.execute(queryMessage);
            queryMessage = new QueryMessage("SELECT * FROM atable",
                                            queryOptions);
            client.execute(queryMessage);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testQueryExecutionWithOverloadedExceptionDisabled() throws Throwable
    {
        SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(),
                                               nativePort,
                                               ProtocolVersion.V5,
                                               true,
                                               new EncryptionOptions());

        try
        {
            client.connect(false, false, false);
            QueryOptions queryOptions = QueryOptions.create(
            QueryOptions.DEFAULT.getConsistency(),
            QueryOptions.DEFAULT.getValues(),
            QueryOptions.DEFAULT.skipMetadata(),
            QueryOptions.DEFAULT.getPageSize(),
            QueryOptions.DEFAULT.getPagingState(),
            QueryOptions.DEFAULT.getSerialConsistency(),
            ProtocolVersion.V5,
            KEYSPACE);

            QueryMessage queryMessage = new QueryMessage("CREATE TABLE atable (pk int PRIMARY KEY, v text)",
                                                         queryOptions);
            client.execute(queryMessage);
            queryMessage = new QueryMessage("SELECT * FROM atable",
                                            queryOptions);
            client.execute(queryMessage);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testOverloadedExceptionForPerChannelInflightLimit() throws Throwable
    {
        SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(),
                                               nativePort,
                                               ProtocolVersion.V5,
                                               true,
                                               new EncryptionOptions());

        try
        {
            client.connect(false, false, true);
            QueryOptions queryOptions = QueryOptions.create(
            QueryOptions.DEFAULT.getConsistency(),
            QueryOptions.DEFAULT.getValues(),
            QueryOptions.DEFAULT.skipMetadata(),
            QueryOptions.DEFAULT.getPageSize(),
            QueryOptions.DEFAULT.getPagingState(),
            QueryOptions.DEFAULT.getSerialConsistency(),
            ProtocolVersion.V5,
            KEYSPACE);

            DatabaseDescriptor.setMaxInflightRequestsPayloadPerChannelInBytes(1000);
            // set inflight request payload per channel to a value such that consequent request should exceed threshold
            Message.Dispatcher.getRequestPayloadInFlightPerChannel().get(client.channel).addAndGet(999);

            QueryMessage queryMessage = new QueryMessage("SELECT * FROM atable",
                                                         queryOptions);

            // we expect this request to be discarded
            // TODO: Exception is not getting bubbled up. Inside client.execute() method, it gets stuck waiting for response.
            // On the server side, ExceptionHandler is not executing the exception.
            Message.Response response = client.execute(queryMessage);
        }
        catch(Exception ex)
        {
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testOverloadedExceptionForOverallInflightLimit() throws Throwable
    {
    }

    @Test
    public void testOverloadedExceptionForInflightAndOverallInflightLimit() throws Throwable
    {
    }
}
