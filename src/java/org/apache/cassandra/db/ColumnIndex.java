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

import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnIndex
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnIndex.class);

    public final long partitionHeaderLength;
    public final List<IndexHelper.IndexInfo> columnsIndex;

    private static final ColumnIndex EMPTY = new ColumnIndex(-1, Collections.<IndexHelper.IndexInfo>emptyList());

    private ColumnIndex(long partitionHeaderLength, List<IndexHelper.IndexInfo> columnsIndex)
    {
        assert columnsIndex != null;

        this.partitionHeaderLength = partitionHeaderLength;
        this.columnsIndex = columnsIndex;
    }

    public static ColumnIndex writeAndBuildIndex(UnfilteredRowIterator iterator, SequentialWriter output, SerializationHeader header, Version version) throws IOException
    {
        assert !iterator.isEmpty() && version.storeRows();

        Builder builder = new Builder(iterator, output, header, version.correspondingMessagingVersion());
        return builder.build();
    }

    @VisibleForTesting
    public static ColumnIndex nothing()
    {
        return EMPTY;
    }

    /**
     * Help to create an index for a column family based on size of columns,
     * and write said columns to disk.
     */
    private static class Builder
    {
        private final UnfilteredRowIterator iterator;
        private final SequentialWriter writer;
        private final SerializationHeader header;
        private final int version;

        private List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<>();
        private final long initialPosition;
        private long headerLength = -1;

        private long startPosition = -1;

        private int written;
        private long previousRowStart;
        private long columnsIndexMemory; // This is only used during building, and should not be used once built.

        private ClusteringPrefix firstClustering;
        private ClusteringPrefix lastClustering;

        private DeletionTime openMarker;

        public Builder(UnfilteredRowIterator iterator,
                       SequentialWriter writer,
                       SerializationHeader header,
                       int version)
        {
            this.iterator = iterator;
            this.writer = writer;
            this.header = header;
            this.version = version;
            this.initialPosition = writer.position();
        }

        private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
            DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer);
            if (header.hasStatic())
                UnfilteredSerializer.serializer.serializeStaticRow(iterator.staticRow(), header, writer, version);
        }

        public ColumnIndex build() throws IOException
        {
            writePartitionHeader(iterator);
            this.headerLength = writer.position() - initialPosition;

            while (iterator.hasNext())
                add(iterator.next());

            return close();
        }

        private long currentPosition()
        {
            return writer.position() - initialPosition;
        }

        private void addIndexBlock()
        {
            IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstClustering,
                                                                         lastClustering,
                                                                         startPosition,
                                                                         currentPosition() - startPosition,
                                                                         openMarker);
            columnsIndex.add(cIndexInfo);
            firstClustering = null;
            columnsIndexMemory += cIndexInfo.unsharedHeapSize();
        }

        private void add(Unfiltered unfiltered) throws IOException
        {
            long pos = currentPosition();

            if (firstClustering == null)
            {
                // Beginning of an index block. Remember the start and position
                firstClustering = unfiltered.clustering();
                startPosition = pos;
            }

            UnfilteredSerializer.serializer.serialize(unfiltered, header, writer, pos - previousRowStart, version);
            lastClustering = unfiltered.clustering();
            previousRowStart = pos;
            ++written;

            if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker)unfiltered;
                openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
            }

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
                addIndexBlock();

        }

        private ColumnIndex close() throws IOException
        {
            UnfilteredSerializer.serializer.writeEndOfPartition(writer);

            // It's possible we add no rows, just a top level deletion
            if (written == 0)
                return ColumnIndex.EMPTY;

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (firstClustering != null)
                addIndexBlock();

            // we should always have at least one computed index block, but we only write it out if there is more than that.
            assert columnsIndex.size() > 0 && headerLength >= 0;

            // Do we need to downsample?
            if (columnsIndexMemory > DatabaseDescriptor.getColumnIndexMaxSizeInBytes()
                || columnsIndex.size() > DatabaseDescriptor.getColumnIndexMaxCount())
            {
                int mergeFactor = Math.max(Ints.checkedCast(columnsIndexMemory) / DatabaseDescriptor.getColumnIndexMaxSizeInBytes(),
                                           columnsIndex.size() / DatabaseDescriptor.getColumnIndexMaxCount());


                if (mergeFactor > 1)
                {
                    logger.info("Downsampling column index ({}/{} bytes, {}/{} items, merge factor {})",
                                columnsIndexMemory, DatabaseDescriptor.getColumnIndexMaxSizeInBytes(),
                                columnsIndex.size(), DatabaseDescriptor.getColumnIndexMaxCount(), mergeFactor);
                    columnsIndex = downsampleColumnIndexList(columnsIndex, mergeFactor);
                    logger.info("Downsampled column index to {} elements ", columnsIndex.size());
                    StorageMetrics.columnIndexDownsamples.inc();
                }

            }
            columnsIndexMemory = -1;
            return new ColumnIndex(headerLength, columnsIndex);
        }
    }

    /**
      * Downsample the provided list by a the scaling factor, merging adjacent records as needed
      *
      * @param indexList List of IndexInfo objects that will be scaled back (in place)
      * @param mergeFactor integer representing the number of adjacent IndexInfo objects to merge
      */
    @VisibleForTesting
    static List<IndexHelper.IndexInfo> downsampleColumnIndexList(List<IndexHelper.IndexInfo> indexList, int mergeFactor)
    {
        Preconditions.checkArgument(mergeFactor >= 2, "Invalid attempt to downsample column index by merge factor less than 2: ", mergeFactor);

        // Presize the downsampled list
        int finalSize = indexList.size() / mergeFactor;
        if (indexList.size() % mergeFactor > 0)
        finalSize = finalSize + 1;

        List <IndexHelper.IndexInfo> downsampled = new ArrayList<>(finalSize);

        // If the original list size is not evenly divisible by the merge factor,
        // the last element of the downsampled list will be smaller, being comprised
        // of the modulo elements (which could be a single element from the original list).
        for (int l = 0; l < indexList.size(); l += mergeFactor)
        {
            int r = Math.min(l + mergeFactor - 1, indexList.size() - 1);

            IndexHelper.IndexInfo left = indexList.get(l);
            IndexHelper.IndexInfo right = indexList.get(r);

            left.expandInPlace(right);
            downsampled.add(left);

            for (int i = l; i < r + 1; i++)
                indexList.set(i, null);
        }

         assert finalSize == downsampled.size();

         return downsampled;
    }

}
