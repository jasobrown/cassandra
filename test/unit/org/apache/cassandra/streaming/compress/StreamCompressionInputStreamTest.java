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

package org.apache.cassandra.streaming.compress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.cassandra.streaming.messages.StreamMessage;

public class StreamCompressionInputStreamTest
{
    private ByteBuf buf;

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void rebuffer_FailAndClose()
    {
        buf = PooledByteBufAllocator.DEFAULT.buffer(8);
        buf.writerIndex(8);

        StreamCompressionInputStream stream = new StreamCompressionInputStream(null, StreamMessage.CURRENT_VERSION);
        stream.currentBuf = buf;
        try
        {
            stream.reBuffer();
            // previous line *should* fail
            Assert.fail("should have failed to read from the stream");
        }
        catch(Exception e)
        {
            // nop
        }

        stream.close();
    }
}
