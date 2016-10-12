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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * IndexedEntry implementation of On-Heap IndexInfo (non B+(ish) Tree based).
 * At a high level IndexInfo objects are serialized and deserialized into a List
 * and then binary searched over to find the offset+width for a given indexed key.
 */
public class OnHeapIndexedEntry implements IndexedEntry
{
    public final long position;
    private final DeletionTime deletionTime;
    private final List<IndexInfo> columnsIndex;
    private static final long BASE_SIZE =
    ObjectSizes.measure(new OnHeapIndexedEntry(0, DeletionTime.LIVE, Arrays.<IndexInfo>asList(null, null)))
    + ObjectSizes.measure(new ArrayList<>(1));

    private int nextIndexIdx = -1;
    private int lastDeserializedBlock = -1;
    private boolean iteratorDirectionReversed = false;

    public OnHeapIndexedEntry(long position, DeletionTime deletionTime, List<IndexInfo> columnsIndex)
    {
        this.position = position;
        assert deletionTime != null;
        assert columnsIndex != null && columnsIndex.size() > 1;
        this.deletionTime = deletionTime;
        this.columnsIndex = columnsIndex;
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return !columnsIndex().isEmpty();
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public List<IndexInfo> columnsIndex()
    {
        return columnsIndex;
    }

    public int entryCount()
    {
        return columnsIndex.size();
    }

    public List<IndexInfo> getAllColumnIndexes()
    {
        return columnsIndex;
    }

    public int promotedSize(CType type)
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        long size = DeletionTime.serializer.serializedSize(deletionTime, typeSizes);
        size += typeSizes.sizeof(columnsIndex.size()); // number of entries
        ISerializer<IndexInfo> idxSerializer = type.indexSerializer();
        for (IndexInfo info : columnsIndex)
            size += idxSerializer.serializedSize(info, typeSizes);

        return Ints.checkedCast(size);
    }

    @Override
    public long unsharedHeapSize()
    {
        long entrySize = 0;
        for (IndexInfo idx : columnsIndex)
            entrySize += idx.unsharedHeapSize();

        return BASE_SIZE
               + entrySize
               + deletionTime.unsharedHeapSize()
               + ObjectSizes.sizeOfReferenceArray(columnsIndex.size());
    }

    public long getPosition()
    {
        return position;
    }

    public IndexInfo getIndexInfo(Composite name, CellNameType comparator, boolean reversed)
    {
        nextIndexIdx = calculateNextIdx(name, comparator, reversed);
        iteratorDirectionReversed = reversed;

        if (nextIndexIdx < 0 || nextIndexIdx >= columnsIndex.size())
            // no index block for that slice
            return null; //todo: returning null is lame

        return columnsIndex.get(nextIndexIdx);
    }

    public boolean hasNext()
    {
        if (lastDeserializedBlock == nextIndexIdx)
        {
            if (iteratorDirectionReversed)
                nextIndexIdx--;
            else
                nextIndexIdx++;
        }

        // Are we done?
        return nextIndexIdx >= 0 && nextIndexIdx < columnsIndex.size();
    }

    public IndexInfo next()
    {
        lastDeserializedBlock = nextIndexIdx;
        return (lastDeserializedBlock < columnsIndex.size()) ? columnsIndex.get(lastDeserializedBlock) : null;
    }

    public IndexInfo peek()
    {
        return (nextIndexIdx + 1 < columnsIndex.size()) ? columnsIndex.get(nextIndexIdx + 1) : null;
    }

    public void startIteratorAt(Composite name, CellNameType comparator, boolean reversed)
    {
        nextIndexIdx = calculateNextIdx(name, comparator, reversed);
        iteratorDirectionReversed = reversed;
    }

    public void close()
    {

    }

    public void reset(boolean reversed)
    {
        iteratorDirectionReversed = reversed;
        lastDeserializedBlock = -1;
        nextIndexIdx = -1;
    }

    public boolean isReversed()
    {
        return iteratorDirectionReversed;
    }

    /**
     * The index of the IndexInfo in which a scan starting with @name should begin.
     *
     * @param name
     *         name of the index
     *
     * @param comparator
     *          comparator type
     *
     * @param reversed
     *          is name reversed
     *
     * @return int index
     */
    public int calculateNextIdx(Composite name, CellNameType comparator, boolean reversed)
    {
        if (name.isEmpty())
            return lastDeserializedBlock >= 0 ? lastDeserializedBlock : reversed ? columnsIndex.size() - 1 : 0;

        if (lastDeserializedBlock >= columnsIndex.size())
            return -1;

        IndexInfo target = new IndexInfo(name, name, 0, 0, comparator);
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we we doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int startIdx = 0;
        List<IndexInfo> toSearch = columnsIndex;
        if (reversed && lastDeserializedBlock >= 0)
        {
            toSearch = columnsIndex.subList(0, lastDeserializedBlock + 1);
        }
        else if (!reversed && lastDeserializedBlock > 0)
        {
            // in the paging case we could have [0-100][101-200][201-300][401-500] with a paging size of 100.
            // If the IndexInfo covering the [101-200] range actually also covers data for some of the [201-300]
            // range, then the next page request also needs to consider the previous IndexInfo; otherwise
            // we'd incorrectly filter out the last returned index.
            startIdx = lastDeserializedBlock - 1;
            toSearch = columnsIndex.subList(lastDeserializedBlock - 1, columnsIndex.size());
        }

        int index = Collections.binarySearch(toSearch, target, getComparator(comparator, reversed));
        return startIdx + (index < 0 ? -index - (reversed ? 2 : 1) : index);
    }

    public static Comparator<IndexInfo> getComparator(final CType nameComparator, boolean reversed)
    {
        return reversed ? nameComparator.indexReverseComparator() : nameComparator.indexComparator();
    }
}
