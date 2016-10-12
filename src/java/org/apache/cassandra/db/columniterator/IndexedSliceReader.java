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
package org.apache.cassandra.db.columniterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * This is a reader that finds the block for a starting column and returns blocks before/after it for each next call.
 * This function assumes that the CF is sorted by name and exploits the name index.
 */
class IndexedSliceReader extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexedSliceReader.class);

    private final ColumnFamily emptyColumnFamily;

    private final SSTableReader sstable;
    private final IndexedEntry indexedEntry;
    private final FileDataInput originalInput;
    private FileDataInput file;
    private final boolean reversed;
    private final ColumnSlice[] slices;
    private final BlockFetcher fetcher;
    private final Deque<OnDiskAtom> blockColumns = new ArrayDeque<>();
    private final CellNameType comparator;

    // Holds range tombstone in reverse queries. See addColumn()
    private final Deque<OnDiskAtom> rangeTombstonesReversed;

    /**
     * This slice reader assumes that slices are sorted correctly, e.g. that for forward lookup slices are in
     * lexicographic order of start elements and that for reverse lookup they are in reverse lexicographic order of
     * finish (reverse start) elements. i.e. forward: [a,b],[d,e],[g,h] reverse: [h,g],[e,d],[b,a]. This reader also
     * assumes that validation has been performed in terms of intervals (no overlapping intervals).
     */
    public IndexedSliceReader(SSTableReader sstable, IndexedEntry indexEntry, FileDataInput input, ColumnSlice[] slices, boolean reversed)
    {
        Tracing.trace("Seeking to partition indexed section in data file");
        this.sstable = sstable;
        this.originalInput = input;
        this.reversed = reversed;
        this.slices = slices;
        this.comparator = sstable.metadata.comparator;
        this.rangeTombstonesReversed = reversed ? new ArrayDeque<OnDiskAtom>() : null;

        try
        {
            this.indexedEntry = indexEntry;
            emptyColumnFamily = ArrayBackedSortedColumns.factory.create(sstable.metadata);
            if (!indexEntry.isIndexed())
            {
                setToRowStart(indexEntry, input);
                emptyColumnFamily.delete(DeletionTime.serializer.deserialize(file));
                fetcher = new SimpleBlockFetcher();
            }
            else
            {
                indexEntry.reset(reversed);
                emptyColumnFamily.delete(indexEntry.deletionTime());
                fetcher = new IndexedBlockFetcher(indexEntry.getPosition());
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file.getPath());
        }
    }

    /**
     * Sets the seek position to the start of the row for column scanning.
     */
    private void setToRowStart(IndexedEntry rowEntry, FileDataInput in) throws IOException
    {
        if (in == null)
        {
            this.file = sstable.getFileDataInput(rowEntry.getPosition());
        }
        else
        {
            this.file = in;
            in.seek(rowEntry.getPosition());
        }
        sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(file));
    }

    public ColumnFamily getColumnFamily()
    {
        return emptyColumnFamily;
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }

    protected OnDiskAtom computeNext()
    {
        while (true)
        {
            if (reversed)
            {
                // Return all tombstone for the block first (see addColumn() below)
                OnDiskAtom column = rangeTombstonesReversed.poll();
                if (column != null)
                    return column;
            }

            OnDiskAtom column = blockColumns.poll();
            if (column == null)
            {
                if (!fetcher.fetchMoreData())
                    return endOfData();
            }
            else
            {
                return column;
            }
        }
    }

    public void close() throws IOException
    {
        if (originalInput == null && file != null)
            file.close();
        indexedEntry.close();
    }

    protected void addColumn(OnDiskAtom col)
    {
        if (reversed)
        {
            /*
             * We put range tomstone markers at the beginning of the range they delete. But for reversed queries,
             * the caller still need to know about a RangeTombstone before it sees any column that it covers.
             * To make that simple, we keep said tombstones separate and return them all before any column for
             * a given block.
             */
            if (col instanceof RangeTombstone)
                rangeTombstonesReversed.addFirst(col);
            else
                blockColumns.addFirst(col);
        }
        else
        {
            blockColumns.addLast(col);
        }
    }

    private abstract class BlockFetcher
    {
        protected int currentSliceIdx;

        protected BlockFetcher(int sliceIdx)
        {
            this.currentSliceIdx = sliceIdx;
        }

        /*
         * Return the smallest key selected by the current ColumnSlice.
         */
        protected Composite currentStart()
        {
            return reversed ? slices[currentSliceIdx].finish : slices[currentSliceIdx].start;
        }

        /*
         * Return the biggest key selected by the current ColumnSlice.
         */
        protected Composite currentFinish()
        {
            return reversed ? slices[currentSliceIdx].start : slices[currentSliceIdx].finish;
        }

        protected abstract boolean fetchMoreData();

        protected boolean isColumnBeforeSliceStart(OnDiskAtom column)
        {
            return isBeforeSliceStart(column.name());
        }

        protected boolean isBeforeSliceStart(Composite name)
        {
            Composite start = currentStart();
            return !start.isEmpty() && comparator.compare(name, start) < 0;
        }

        protected boolean isColumnBeforeSliceFinish(OnDiskAtom column)
        {
            Composite finish = currentFinish();
            return finish.isEmpty() || comparator.compare(column.name(), finish) <= 0;
        }

        protected boolean isAfterSliceFinish(Composite name)
        {
            Composite finish = currentFinish();
            return !finish.isEmpty() && comparator.compare(name, finish) > 0;
        }
    }

    private class IndexedBlockFetcher extends BlockFetcher
    {
        // where this row starts
        private final long columnsStart;

        private IndexInfo lastDeserializedBlock = null;

        // For reversed, keep columns at the beginning of the last deserialized block that
        // may still match a slice
        private final Deque<OnDiskAtom> prefetched;

        public IndexedBlockFetcher(long columnsStart)
        {
            super(-1);
            this.columnsStart = columnsStart;
            this.prefetched = reversed ? new ArrayDeque<OnDiskAtom>() : null;
            getNextIndexedSlice();
        }

        protected IndexInfo getNextIndexedSlice()
        {
            // todo: kjkj this code is extremely fragle and complicated
            // still need to ensure reading the legacy index formats works as expected

            while (++currentSliceIdx < slices.length)
            {
                try
                {
                    indexedEntry.startIteratorAt(slices[currentSliceIdx].start, comparator, reversed);
                    if (!indexedEntry.hasNext())
                        // no index block for that slice
                        continue;

                    IndexInfo info = indexedEntry.peek();
                    if(info == null)
                    {
                        logger.warn("indexed entry peek() is null in getNextIndexedSlice()");
                        continue;
                    }

                    if (reversed)
                    {
                        // todo: kjellman -> handle legacy..
                        if (!isAfterSliceFinish(info.getFirstNameAsComposite()))
                            return info;

                        //if (!isBeforeSliceStart(info.lastName))
                        //return info;
                    }
                    else
                    {
                        if (!isAfterSliceFinish(info.getFirstNameAsComposite()))
                            return info;
                    }
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, file.getPath());
                }
            }

            return null; //todo: kjellman; checking for null all over the place is lame.. use Optional instead?
        }

        protected boolean hasMoreSlice()
        {
            return currentSliceIdx < slices.length;
        }

        protected boolean fetchMoreData()
        {
            if (!hasMoreSlice())
            {
                return false;
            }

            // If we read blocks in reversed disk order, we may have columns from the previous block to handle.
            // Note that prefetched keeps columns in reversed disk order.
            // Also note that Range Tombstone handling is a bit tricky, because we may run into range tombstones
            // that cover a slice *after* we've move to the previous slice. To keep it simple, we simply include
            // every RT in prefetched: it's only slightly inefficient to do so and there is only so much RT that
            // can be mistakenly added this way.
            if (reversed && !prefetched.isEmpty())
            {
                // Avoids some comparison when we know it's not useful
                boolean inSlice = false;

                OnDiskAtom prefetchedCol;
                while ((prefetchedCol = prefetched.peek()) != null)
                {
                    // col is before slice, we update the slice
                    if (isColumnBeforeSliceStart(prefetchedCol))
                    {
                        inSlice = false;

                        // As explained above, we add RT unconditionally
                        if (prefetchedCol instanceof RangeTombstone)
                        {
                            blockColumns.addLast(prefetched.poll());
                            continue;
                        }

                        // Otherwise, we either move to the next slice. If we have no more slice, then
                        // simply unwind prefetched entirely and add all RT.
                        if (getNextIndexedSlice() == null)
                        {
                            while ((prefetchedCol = prefetched.poll()) != null)
                                if (prefetchedCol instanceof RangeTombstone)
                                    blockColumns.addLast(prefetchedCol);
                            break;
                        }

                    }
                    // col is within slice, all columns
                    // (we go in reverse, so as soon as we are in a slice, no need to check
                    // we're after the slice until we change slice)
                    else if (inSlice || isColumnBeforeSliceFinish(prefetchedCol))
                    {
                        blockColumns.addLast(prefetched.poll());
                        inSlice = true;
                    }
                    // if col is after slice, ignore
                    else
                    {
                        prefetched.poll();
                    }
                }

                if (!blockColumns.isEmpty())
                    return true;
                else if (!hasMoreSlice())
                    return false;
            }
            try
            {
                return getNextBlock();
            }
            catch (IOException e)
            {
                throw new CorruptSSTableException(e, file.getPath());
            }
        }

        private boolean getNextBlock() throws IOException
        {
            if (!indexedEntry.hasNext())
            {
                return false;
            }

            IndexInfo currentIndex = indexedEntry.next();

            // check if the last indexed block we added in a previous iteration
            // isn't going to be the same block returned now, otherwise we'll get
            // duplicates. This will happen if you have multiple slices that overlap
            if (lastDeserializedBlock != null && currentIndex.offset == lastDeserializedBlock.offset)
            {
                if (indexedEntry.hasNext())
                    currentIndex = indexedEntry.next();
                else
                    return false;
            }

            lastDeserializedBlock = currentIndex;

            /* seek to the correct offset to the data, and calculate the data size */
            long positionToSeek = columnsStart + currentIndex.offset;

            // With new promoted indexes, our first seek in the data file will happen at that point.
            if (file == null)
                file = originalInput == null ? sstable.getFileDataInput(positionToSeek) : originalInput;

            AtomDeserializer deserializer = emptyColumnFamily.metadata().getOnDiskDeserializer(file, sstable.descriptor.version);

            file.seek(positionToSeek);
            FileMark mark = file.mark();

            // We remenber when we are whithin a slice to avoid some comparison
            boolean inSlice = false;

            // scan from index start
            while (file.bytesPastMark(mark) < currentIndex.width || deserializer.hasUnprocessed())
            {
                // col is before slice
                // (If in slice, don't bother checking that until we change slice)
                Composite start = currentStart();
                if (!inSlice && !start.isEmpty() && deserializer.compareNextTo(start) < 0)
                {
                    // If it's a rangeTombstone, then we need to read it and include it unless it's end
                    // stops before our slice start.
                    if (deserializer.nextIsRangeTombstone())
                    {
                        RangeTombstone rt = (RangeTombstone)deserializer.readNext();
                        if (comparator.compare(rt.max, start) >= 0)
                            addColumn(rt);
                        continue;
                    }

                    if (reversed)
                    {
                        // the next slice select columns that are before the current one, so it may
                        // match this column, so keep it around.
                        prefetched.addFirst(deserializer.readNext());
                    }
                    else
                    {
                        deserializer.skipNext();
                    }
                }
                // col is within slice
                else
                {
                    Composite finish = currentFinish();
                    if (finish.isEmpty() || deserializer.compareNextTo(finish) <= 0)
                    {
                        inSlice = true;
                        addColumn(deserializer.readNext());
                    }
                    // col is after slice.
                    else
                    {
                        // When reading forward, if we hit a column that sorts after the current slice, it means we're done with this slice.
                        // For reversed, this may either mean that we're done with the current slice, or that we need to read the previous
                        // index block. However, we can be sure that we are in the first case though (the current slice is done) if the first
                        // columns of the block were not part of the current slice, i.e. if we have columns in prefetched.
                        if (reversed && prefetched.isEmpty())
                            break;

                        IndexInfo nextIndexedSlice = getNextIndexedSlice();
                        if (nextIndexedSlice == null)
                            break;

                        inSlice = false;

                        // The next index block now corresponds to the first block that may have columns for the newly set slice.
                        // So if it's different from the current block, we're done with this block. And in that case, we know
                        // that our prefetched columns won't match.
                        if (comparator.compare(currentIndex.getFirstNameAsComposite(), nextIndexedSlice.getFirstNameAsComposite()) != 0)
                        {
                            if (reversed)
                                prefetched.clear();
                            break;
                        }

                        // Even if the next slice may have column in this blocks, if we're reversed, those columns have been
                        // prefetched and we're done with that block
                        if (reversed)
                            break;

                        // otherwise, we will deal with that column at the next iteration
                    }
                }
            }
            return true;
        }
    }

    private class SimpleBlockFetcher extends BlockFetcher
    {
        public SimpleBlockFetcher() throws IOException
        {
            // Since we have to deserialize in order and will read all slices might as well reverse the slices and
            // behave as if it was not reversed
            super(reversed ? slices.length - 1 : 0);

            // We remenber when we are whithin a slice to avoid some comparison
            boolean inSlice = false;

            AtomDeserializer deserializer = emptyColumnFamily.metadata().getOnDiskDeserializer(file, sstable.descriptor.version);
            while (deserializer.hasNext())
            {
                // col is before slice
                // (If in slice, don't bother checking that until we change slice)
                Composite start = currentStart();
                if (!inSlice && !start.isEmpty() && deserializer.compareNextTo(start) < 0)
                {
                    // If it's a rangeTombstone, then we need to read it and include it unless it's end
                    // stops before our slice start. Otherwise, we can skip it.
                    if (deserializer.nextIsRangeTombstone())
                    {
                        RangeTombstone rt = (RangeTombstone)deserializer.readNext();
                        if (comparator.compare(rt.max, start) >= 0)
                            addColumn(rt);
                    }
                    else
                    {
                        deserializer.skipNext();
                    }
                    continue;
                }

                // col is within slice
                Composite finish = currentFinish();
                if (finish.isEmpty() || deserializer.compareNextTo(finish) <= 0)
                {
                    inSlice = true;
                    addColumn(deserializer.readNext());
                }
                // col is after slice. more slices?
                else
                {
                    inSlice = false;
                    if (!setNextSlice())
                        break;
                }
            }
        }

        protected boolean setNextSlice()
        {
            if (reversed)
            {
                if (currentSliceIdx <= 0)
                    return false;

                currentSliceIdx--;
            }
            else
            {
                if (currentSliceIdx >= slices.length - 1)
                    return false;

                currentSliceIdx++;
            }
            return true;
        }

        protected boolean fetchMoreData()
        {
            return false;
        }
    }
}
