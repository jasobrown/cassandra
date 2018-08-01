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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class StreamingByteBuf extends AbstractByteBuf
{
    private final RebufferingByteBufDataInputPlus rebufferingQueues;

    public StreamingByteBuf(RebufferingByteBufDataInputPlus rebufferingQueues)
    {
        this.rebufferingQueues = rebufferingQueues;
    }


    @Override
    protected byte _getByte(int index)
    {
        return rebufferingQueues.readByte();
    }

    @Override
    protected short _getShort(int index)
    {
        return 0;
    }

    @Override
    protected short _getShortLE(int index)
    {
        return 0;
    }

    @Override
    protected int _getUnsignedMedium(int index)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int _getUnsignedMediumLE(int index)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int _getInt(int index)
    {
        return 0;
    }

    @Override
    protected int _getIntLE(int index)
    {
        return 0;
    }

    @Override
    protected long _getLong(int index)
    {
        return 0;
    }

    @Override
    protected long _getLongLE(int index)
    {
        return rebufferingQueues.readLong();
    }

    @Override
    protected void _setByte(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setShort(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setShortLE(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setMedium(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setMediumLE(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setInt(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setIntLE(int index, int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setLong(int index, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setLongLE(int index, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int capacity()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public ByteBuf capacity(int newCapacity)
    {
        return this;
    }

    @Override
    public ByteBufAllocator alloc()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteOrder order()
    {
        return null;
    }

    @Override
    public ByteBuf unwrap()
    {
        return null;
    }

    @Override
    public boolean isDirect()
    {
        return false;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length)
    {
        return null;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length)
    {
        return null;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst)
    {
        return null;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException
    {
        return null;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException
    {
        return 0;
    }

    @Override
    public int getBytes(int index, FileChannel out, long position, int length) throws IOException
    {
        return 0;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setBytes(int index, FileChannel in, long position, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf copy(int index, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nioBufferCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasArray()
    {
        return false;
    }

    @Override
    public byte[] array()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int arrayOffset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasMemoryAddress()
    {
        return false;
    }

    @Override
    public long memoryAddress()
    {
        return 0;
    }

    @Override
    public ByteBuf retain(int increment)
    {
        return null;
    }

    @Override
    public int refCnt()
    {
        return 0;
    }

    @Override
    public ByteBuf retain()
    {
        return null;
    }

    @Override
    public ByteBuf touch()
    {
        return null;
    }

    @Override
    public ByteBuf touch(Object hint)
    {
        return null;
    }

    @Override
    public boolean release()
    {
        return false;
    }

    @Override
    public boolean release(int decrement)
    {
        return false;
    }
}
