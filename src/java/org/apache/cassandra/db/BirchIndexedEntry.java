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
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.index.birch.BirchReader;
import org.apache.cassandra.db.index.birch.PageAlignedReader;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An IndexedEntry implementation that is backed by a BirchWriter Index
 */
public class BirchIndexedEntry implements IndexedEntry, AutoCloseable
{
    private final long position;
    private final CType type;
    private PageAlignedReader reader;
    private final BirchReader<IndexInfo> birchReader;
    private final DeletionTime deletionTime;
    private final int readerSegmentIdx;
    private final short readerSubSegmentIdx;

    private BirchReader<IndexInfo>.BirchIterator iterator = null;
    private boolean iteratorDirectionReversed = false;

    public BirchIndexedEntry(long position, CType type, PageAlignedReader reader, DeletionTime deletionTime) throws IOException
    {
        this.position = position;
        this.type = type;
        this.reader = reader;
        this.deletionTime = deletionTime;
        this.birchReader = new BirchReader<>(reader);
        this.readerSegmentIdx = reader.getCurrentSegmentIdx();
        this.readerSubSegmentIdx = reader.getCurrentSubSegmentIdx();
    }

    public boolean isIndexed()
    {
        return true;
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public int promotedSize(CType type)
    {
        return 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        return 0;
    }

    public long getPosition()
    {
        return position;
    }

    public int entryCount()
    {
        return birchReader.getElementCount();
    }

    public List<IndexInfo> getAllColumnIndexes()
    {
        throw new UnsupportedOperationException();
    }

    public IndexInfo getIndexInfo(Composite name, CellNameType comparator, boolean reversed) throws IOException
    {
        assert reader.isCurrentSubSegmentPageAligned()
               && reader.getCurrentSegmentIdx() == readerSegmentIdx;

        iteratorDirectionReversed = reversed;
        return birchReader.search(name, comparator, reversed);
    }

    public static Comparator<IndexInfo> getComparator(final CType nameComparator, boolean reversed)
    {
        return reversed ? nameComparator.indexReverseComparator() : nameComparator.indexComparator();
    }

    public boolean hasNext()
    {
        assert reader.getCurrentSegmentIdx() == readerSegmentIdx;
        return iterator != null && iterator.hasNext();
    }

    public IndexInfo next()
    {
        assert reader.getCurrentSegmentIdx() == readerSegmentIdx;
        return (iterator != null) ? iterator.next() : null;
    }

    public IndexInfo peek()
    {
        return (iterator != null) ? iterator.peek() : null;
    }

    public void startIteratorAt(Composite name, CellNameType comparator, boolean reversed) throws IOException
    {
        assert reader.getCurrentSegmentIdx() == readerSegmentIdx;

        iteratorDirectionReversed = reversed;
        iterator = birchReader.getIterator(name, comparator, reversed);
    }

    public boolean isReversed()
    {
        return iteratorDirectionReversed;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(birchReader);
        FileUtils.closeQuietly(reader);
    }

    public void reset(boolean reversed)
    {
        try
        {
            if (reader.isClosed())
            {
                PageAlignedReader newReader = PageAlignedReader.copy(reader);
                reader = newReader;
                birchReader.unsafeReplaceReader(reader);
            }

            // the birch index is always in the 2nd sub-segment
            reader.setSegment(readerSegmentIdx, 1);
            iterator = birchReader.getIterator(type, reversed);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        iteratorDirectionReversed = reversed;
    }
}
