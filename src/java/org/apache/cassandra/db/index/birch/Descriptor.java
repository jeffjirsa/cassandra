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

package org.apache.cassandra.db.index.birch;

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.db.index.birch.BirchWriter.CURRENT_VERSION;
import static org.apache.cassandra.db.index.birch.BirchWriter.SerializerType;

/**
 * Descriptor for a Birch tree serialized by {@link org.apache.cassandra.db.index.birch.BirchWriter}.
 * It contains the required offsets and parameters used to serialize, deserialize,
 * and read from the tree.
 *
 * @see BirchWriter
 */
public class Descriptor
{
    private final short version;
    private final long rootOffset;
    private final SerializerType serializerType;
    private final long firstNodeOffset;
    private final long firstLeafOffset;
    private final long overflowPageOffset;
    private final long overflowPageLength;
    private final int alignedPageSize;
    private final int elementCount;

    private Descriptor(Builder builder)
    {
        this.version = builder.version;
        this.alignedPageSize = builder.alignedPageSize;
        this.serializerType = builder.serializerType;
        this.rootOffset = builder.rootOffset;
        this.firstNodeOffset = builder.firstNodeOffset;
        this.firstLeafOffset = builder.firstLeafOffset;
        this.overflowPageOffset = builder.overflowPageOffset;
        this.overflowPageLength = builder.overflowPageLength;
        this.elementCount = builder.elementCount;
    }

    public static class Builder
    {
        private final short version;
        private final int alignedPageSize;
        private final SerializerType serializerType;
        private long rootOffset = 0L;
        private long firstNodeOffset = 0L;
        private long firstLeafOffset = 0L;
        private long overflowPageOffset = 0L;
        private long overflowPageLength = 0L;
        private int elementCount = 0;

        public Builder(SerializerType serializerType)
        {
            this(CURRENT_VERSION, DatabaseDescriptor.getSSTableIndexSegmentPaddingLength(), serializerType);
        }

        public Builder(int alignedPageSize, SerializerType serializerType)
        {
            this(CURRENT_VERSION, alignedPageSize, serializerType);
        }

        public Builder(short version, int alignedPageSize, SerializerType serializerType)
        {
            this.version = version;
            this.alignedPageSize = alignedPageSize;
            this.serializerType = serializerType;
        }

        public Builder rootOffset(long rootOffset)
        {
            this.rootOffset = rootOffset;
            return this;
        }

        public Builder firstNodeOffset(long firstNodeOffset)
        {
            this.firstNodeOffset = firstNodeOffset;
            return this;
        }

        public Builder firstLeafOffset(long firstLeafOffset)
        {
            this.firstLeafOffset = firstLeafOffset;
            return this;
        }

        public Builder overflowPageOffset(long overflowPageOffset)
        {
            this.overflowPageOffset = overflowPageOffset;
            return this;
        }

        public Builder overflowPageLength(long overflowPageLength)
        {
            this.overflowPageLength = overflowPageLength;
            return this;
        }

        public Builder elementCount(int elementCount)
        {
            this.elementCount = elementCount;
            return this;
        }

        public Descriptor build()
        {
            return new Descriptor(this);
        }
    }

    public short getVersion()
    {
        return version;
    }

    public long getRootOffset()
    {
        return rootOffset;
    }

    public long getFirstNodeOffset()
    {
        return firstNodeOffset;
    }

    public long getFirstLeafOffset()
    {
        return firstLeafOffset;
    }

    public long getOverflowPageOffset()
    {
        return overflowPageOffset;
    }

    public long getOverflowPageLength()
    {
        return overflowPageLength;
    }

    public int getAlignedPageSize()
    {
        return alignedPageSize;
    }

    public int getElementCount()
    {
        return elementCount;
    }

    public SerializerType getSerializerType()
    {
        return serializerType;
    }

    public void serialize(PageAlignedWriter writer) throws IOException
    {
        long pos = writer.getCurrentFilePosition();

        writer.writeShort(version);
        writer.writeInt(alignedPageSize);
        writer.writeShort(serializerType.getId());
        writer.writeLong(rootOffset);
        writer.writeLong(firstNodeOffset);
        writer.writeLong(firstLeafOffset);
        writer.writeLong(overflowPageOffset);
        writer.writeLong(overflowPageLength);
        writer.writeInt(elementCount);

        writer.seek(pos + alignedPageSize);
    }

    public static Descriptor deserialize(PageAlignedReader reader) throws IOException
    {
        short version = reader.readShort();
        int alignedPageSize = reader.readInt();
        SerializerType serializerType = SerializerType.getById(reader.readShort());
        Builder builder = new Builder(version, alignedPageSize, serializerType)
                          .rootOffset(reader.readLong())
                          .firstNodeOffset(reader.readLong())
                          .firstLeafOffset(reader.readLong())
                          .overflowPageOffset(reader.readLong())
                          .overflowPageLength(reader.readLong())
                          .elementCount(reader.readInt());
        return builder.build();
    }
}
