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

package org.apache.cassandra.io.sstable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.index.birch.IndexInfoSerializer;
import org.apache.cassandra.db.index.birch.PageAlignedWriter;
import org.apache.cassandra.db.index.birch.TreeSerializable;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class IndexInfo implements TreeSerializable
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, 0, 0, null));

    public static final IndexInfoSerializer SERIALIZER = new IndexInfoSerializer();

    public final long offset;
    public final long width;
    private final ByteBuffer firstName;
    private final ByteBuffer lastName;
    private final CType type;

    public IndexInfo(Composite firstName, Composite lastName, long offset, long width, CType type)
    {
        this(firstName.toByteBuffer(), (lastName == null) ? null : lastName.toByteBuffer(), offset, width, type);
    }

    public IndexInfo(ByteBuffer firstName, ByteBuffer lastName, long offset, long width, CType type)
    {
        this.firstName = firstName;
        this.lastName = lastName;
        this.offset = offset;
        this.width = width;
        this.type = type;
    }

    public IndexInfo(ByteBuffer firstName, long offset, long width, CType type)
    {
        this(firstName, null, offset, width, type);
    }

    public static class Serializer implements ISerializer<IndexInfo>
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serialize(IndexInfo info, DataOutputPlus out) throws IOException
        {
            type.serializer().serialize(type.fromByteBuffer(info.getFirstName()), out);
            type.serializer().serialize(type.fromByteBuffer(info.getLastName()), out);
            out.writeLong(info.offset);
            out.writeLong(info.width);
        }

        public IndexInfo deserialize(DataInput in) throws IOException
        {
            return new IndexInfo(type.serializer().deserialize(in),
                                 type.serializer().deserialize(in),
                                 in.readLong(),
                                 in.readLong(),
                                 type);
        }

        public long serializedSize(IndexInfo info, TypeSizes typeSizes)
        {
            return type.serializer().serializedSize(info.getFirstNameAsComposite(), typeSizes)
                   + type.serializer().serializedSize(info.getLastNameAsComposite(), typeSizes)
                   + typeSizes.sizeof(info.offset)
                   + typeSizes.sizeof(info.width);
        }
    }

    public ByteBuffer getFirstName()
    {
        return firstName.duplicate();
    }

    public Composite getFirstNameAsComposite()
    {
        return type.fromByteBuffer(firstName.duplicate());
    }

    public ByteBuffer getLastName()
    {
        return (lastName == null) ? firstName : lastName;
    }

    public Composite getLastNameAsComposite()
    {
        return (lastName == null) ? getFirstNameAsComposite() : type.fromByteBuffer(lastName.duplicate());
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + getFirstNameAsComposite().unsharedHeapSize() + getLastNameAsComposite().unsharedHeapSize();
    }

    public ByteBuffer serializedKey(CType type) {
        return getFirstName();
    }

    public void serializeValue(PageAlignedWriter writer) throws IOException {
        SERIALIZER.serializeValue(this, writer);
    }

    public int serializedKeySize(CType type) {
        ByteBuffer key = serializedKey(type);
        return key.limit() - key.position();
    }

    public int serializedValueSize() {
        return Long.BYTES + Long.BYTES;
    }

    @Override
    public String toString()
    {
        return String.format("firstName: %s lastName: %S width: %d offset: %d",
                             ByteBufferUtil.bytesToHex(getFirstName()),
                             ByteBufferUtil.bytesToHex(getLastName()),
                             width,
                             offset);
    }
}
