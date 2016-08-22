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

package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.index.birch.BirchWriter;
import org.apache.cassandra.db.index.birch.PageAlignedReader;
import org.apache.cassandra.db.index.birch.PageAlignedWriter;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.db.index.birch.BirchWriter.SerializerType;

public interface IndexedEntry extends IMeasurableMemory, Iterator<IndexInfo>
{
    IndexInfo getIndexInfo(Composite name, CellNameType comparator, boolean reversed) throws IOException;

    void startIteratorAt(Composite name, CellNameType comparator, boolean reversed) throws IOException;

    void reset(boolean reversed);

    void close();

    boolean isReversed();

    IndexInfo peek();

    long getPosition();

    boolean isIndexed();

    int entryCount();

    int promotedSize(CType type);

    DeletionTime deletionTime();

    List<IndexInfo> getAllColumnIndexes();

    class Serializer
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void legacySerialize(IndexedEntry rie, DataOutputPlus out) throws IOException
        {
            out.writeLong(rie.getPosition());
            out.writeInt(rie.getAllColumnIndexes().size());

            if (rie.isIndexed())
            {
                assert rie instanceof OnHeapIndexedEntry;

                DeletionTime.serializer.serialize(rie.deletionTime(), out);

                out.writeInt(rie.getAllColumnIndexes().size());
                for (IndexInfo info : rie.getAllColumnIndexes()) {
                    type.indexSerializer().serialize(info, out);
                }
            }
        }

        public void serialize(IndexedEntry rie, PageAlignedWriter writer) throws IOException
        {
            writer.writeBoolean(rie.isIndexed()); // is following serializated index entry indexed?
            writer.writeLong(rie.getPosition());

            if (rie.isIndexed())
            {
                DeletionTime.serializer.serialize(rie.deletionTime(), writer.stream);
                writer.finalizeCurrentSubSegment();
                BirchWriter birchWriter = new BirchWriter.Builder<>(rie.getAllColumnIndexes().iterator(), SerializerType.INDEXINFO, type).build();
                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
            else
            {
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        public IndexedEntry deserialize(DataInput in, Descriptor.Version version) throws IOException
        {
            // instanceof check is a "hack" for now to use the "old"/legacy/pre-birch deserialization
            // logic with the AutoSavingCache
            if (version.hasBirchIndexes && in instanceof PageAlignedReader)
            {
                PageAlignedReader reader = (PageAlignedReader) in;

                boolean isIndexed = reader.readBoolean();
                long position = reader.readLong();

                if (isIndexed)
                {
                    DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
                    reader.nextSubSegment();
                    IndexedEntry entry = new BirchIndexedEntry(position, type, reader, deletionTime);
                    reader.seekToEndOfCurrentSubSegment();
                    return entry;
                }
                else
                {
                    return new NonIndexedRowEntry(position);
                }
            }
            else
            {
                long position = in.readLong();
                int size = in.readInt();
                if (size > 0)
                {
                    DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                    int entries = in.readInt();
                    ISerializer<IndexInfo> idxSerializer = type.indexSerializer();
                    List<IndexInfo> columnsIndex = new ArrayList<>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(idxSerializer.deserialize(in));

                    return new OnHeapIndexedEntry(position, deletionTime, columnsIndex);
                }
                else
                {
                    return new NonIndexedRowEntry(position);
                }
            }
        }

        public static void skip(DataInput in, Descriptor.Version version) throws IOException
        {
            if (version.hasBirchIndexes)
            {
                PageAlignedReader reader = (PageAlignedReader) in;

                // todo: kjkj can this be cleaned up/simplified? e.g. i think the logic should/could be,
                // always set to start of next segment unless current segment is the last segment, in
                // which case seek to the end of the current segment
                if (reader.getCurrentSegmentIdx() == reader.numberOfSegments() - 1)
                {
                    // if the current sub-segment is the last available sub-segment (and current is the last segment)
                    // seek to the end of the sub-segment to ensure isEOF will return true regardless of iteration order
                    if (reader.getCurrentSubSegmentIdx() == reader.numberOfSubSegments() - 1)
                    {
                        reader.seekToEndOfCurrentSegment();
                    }
                    else
                    {
                        // skip to the beginning of the next sub-segment
                        reader.setSegment(reader.getCurrentSegmentIdx(), reader.getCurrentSubSegmentIdx() + 1);
                    }
                }
                else
                {
                    reader.setSegment(reader.getCurrentSegmentIdx() + 1);
                }
            }
            else
            {
                in.readLong();
                skipPromotedIndex(in, version);
            }
        }

        private static void skipPromotedIndex(DataInput in, Descriptor.Version version) throws IOException
        {
            int size = in.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(in, size);
        }

        public int serializedSize(IndexedEntry rie)
        {
            int size = TypeSizes.NATIVE.sizeof(rie.getPosition()) + TypeSizes.NATIVE.sizeof(rie.promotedSize(type));

            if (rie.isIndexed())
            {
                List<IndexInfo> index = rie.getAllColumnIndexes();

                size += DeletionTime.serializer.serializedSize(rie.deletionTime(), TypeSizes.NATIVE);
                size += TypeSizes.NATIVE.sizeof(index.size());

                ISerializer<IndexInfo> idxSerializer = type.indexSerializer();
                for (IndexInfo info : index)
                    size += idxSerializer.serializedSize(info, TypeSizes.NATIVE);
            }

            return size;
        }
    }
}
