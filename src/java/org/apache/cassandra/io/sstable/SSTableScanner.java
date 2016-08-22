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

import java.io.IOException;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexedEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IColumnIteratorFactory;
import org.apache.cassandra.db.columniterator.LazyColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.index.birch.PageAlignedReader;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.AbstractBounds.Boundary;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

public class SSTableScanner implements ISSTableScanner
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableScanner.class);

    protected final RandomAccessReader dfile;
    private final FileDataInput ifile;
    public final SSTableReader sstable;

    private final Iterator<AbstractBounds<RowPosition>> rangeIterator;
    private AbstractBounds<RowPosition> currentRange;

    private final DataRange dataRange;

    protected Iterator<OnDiskAtomIterator> iterator;

    public static ISSTableScanner getScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        return new SSTableScanner(sstable, dataRange, limiter);
    }
    public static ISSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(tokenRanges);
        if (positions.isEmpty())
            return new EmptySSTableScanner(sstable.getFilename());

        return new SSTableScanner(sstable, tokenRanges, limiter);
    }

    /**
     * @param sstable SSTable to scan; must not be null
     * @param dataRange a single range to scan; must not be null
     * @param limiter background i/o RateLimiter; may be null
     */
    private SSTableScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = dataRange;

        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(2);
        addRange(dataRange.keyRange(), boundsList);
        this.rangeIterator = boundsList.iterator();
    }

    /**
     * @param sstable SSTable to scan; must not be null
     * @param tokenRanges A set of token ranges to scan
     * @param limiter background i/o RateLimiter; may be null
     */
    private SSTableScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = null;

        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(tokenRanges.size());
        for (Range<Token> range : Range.normalize(tokenRanges))
            addRange(range.toRowBounds(), boundsList);

        this.rangeIterator = boundsList.iterator();
    }

    private void addRange(AbstractBounds<RowPosition> requested, List<AbstractBounds<RowPosition>> boundsList)
    {
        if (requested instanceof Range && ((Range)requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.first) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                Boundary<RowPosition> left = new Boundary<RowPosition>(sstable.first, true);
                Boundary<RowPosition> right;
                right = requested.rightBoundary();
                right = minRight(right, sstable.last, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.last) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                Boundary<RowPosition> right = new Boundary<RowPosition>(sstable.last, true);
                Boundary<RowPosition> left;
                left = requested.leftBoundary();
                left = maxLeft(left, sstable.first, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            assert requested.left.compareTo(requested.right) <= 0 || requested.right.isMinimum();
            Boundary<RowPosition> left, right;
            left = requested.leftBoundary();
            right = requested.rightBoundary();
            left = maxLeft(left, sstable.first, true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = requested.right.isMinimum() ? new Boundary<RowPosition>(sstable.last, true)
                                                    : minRight(right, sstable.last, true);
            if (!isEmpty(left, right))
                boundsList.add(AbstractBounds.bounds(left, right));
        }
    }

    private void seekToCurrentRangeStart()
    {
        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        try
        {
            if (ifile instanceof PageAlignedReader)
                ((PageAlignedReader) ifile).findAndSetSegmentAndSubSegmentCurrentForPosition(indexPosition);

            ifile.seek(indexPosition);
            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                if (indexDecoratedKey.compareTo(currentRange.left) > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    // If the sstable has birch indexes, skip the serialized "is indexed" marker
                    if (sstable.descriptor.version.hasBirchIndexes)
                        ifile.readBoolean();

                    long dataPosition = ifile.readLong();

                    if (ifile instanceof PageAlignedReader)
                        ((PageAlignedReader) ifile).findAndSetSegmentAndSubSegmentCurrentForPosition(indexPosition);

                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    IndexedEntry.Serializer.skip(ifile, sstable.descriptor.version);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public void close() throws IOException
    {
        if (iterator != null)
            FileUtils.closeQuietly((AutoCloseable) iterator);

        FileUtils.close(dfile, ifile);
    }

    public long getLengthInBytes()
    {
        return dfile.length();
    }

    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public String getBackingFiles()
    {
        return sstable.toString();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public OnDiskAtomIterator next()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private Iterator<OnDiskAtomIterator> createIterator()
    {
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<OnDiskAtomIterator> implements AutoCloseable
    {
        private DecoratedKey nextKey;
        private IndexedEntry nextEntry;
        private DecoratedKey currentKey;
        private IndexedEntry currentEntry;
        private long lastDeserialziedPos;
        private FileMark lastDeserializedMark;
        
        protected OnDiskAtomIterator computeNext()
        {
            try
            {
                // there are some cases where the iterator will be advanced more than once,
                // so we have to do this check to make sure we're "starting" at the same offset
                // we "last" deserialized for the next iteration.
                if (lastDeserialziedPos > 0)
                {
                    if (ifile instanceof RandomAccessReader)
                    {
                        // handle legacy index format
                        ifile.reset(lastDeserializedMark);
                    }
                    else
                    {
                        if (((PageAlignedReader) ifile).isPositionInsideCurrentSegment(lastDeserialziedPos))
                        {
                            if (((PageAlignedReader) ifile).hasNextSegment())
                            {
                                try
                                {
                                    ((PageAlignedReader) ifile).nextSegment();
                                }
                                catch (Exception e)
                                {
                                    Exception closeE = ((PageAlignedReader) ifile).getCloseException();
                                    if (closeE != null)
                                    {
                                        logger.error("Attempted to call nextSegment() on a closed PageAlignedReader. Originally closed " +
                                                     "by the following stack trace", closeE);
                                    }
                                    else
                                    {
                                        logger.error("Attempted to call nextSegment() on a closed PageAlignedReader but the closeException " +
                                                     "was null");
                                    }
                                    throw e;
                                }
                            }
                        }
                        else
                        {
                            ifile.reset(lastDeserializedMark);
                        }
                    }
                }

                if (nextEntry == null)
                {
                    do
                    {
                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                        {
                            maybeCloseCurrentIndexEntry();
                            maybeCloseNextIndexEntry();

                            return endOfData();
                        }

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();

                        if (ifile.isEOF())
                        {
                            maybeCloseCurrentIndexEntry();
                            maybeCloseNextIndexEntry();

                            return endOfData();
                        }

                        currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = sstable.metadata.comparator.rowIndexEntrySerializer().deserialize(ifile, sstable.descriptor.version);

                        lastDeserialziedPos = (ifile instanceof RandomAccessReader) ? ifile.getFilePointer() : ((PageAlignedReader)ifile).getOffset();
                        lastDeserializedMark = ifile.mark();
                    } while (!currentRange.contains(currentKey));
                }
                else
                {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                long readEnd;
                if (ifile.isEOF())
                {
                    maybeCloseNextIndexEntry();

                    nextEntry = null;
                    nextKey = null;
                    readEnd = dfile.length();
                }
                else
                {
                    if (ifile instanceof PageAlignedReader && ifile.isCurrentSegmentExausted())
                        ((PageAlignedReader)ifile).nextSegment();

                    // todo: kj... i'm using isCurrentSubSegmentPageAligned as a way to differ between
                    // a birch segment and a normal index.. this is pretty lame.. can i do better?
                    if (ifile instanceof PageAlignedReader && ((PageAlignedReader)ifile).isCurrentSubSegmentPageAligned())
                    {
                        lastDeserialziedPos = ((PageAlignedReader)ifile).getOffset();
                        lastDeserializedMark = ifile.mark();
                        readEnd = currentEntry.getPosition();
                    }
                    else
                    {
                        // we need the position of the start of the next key, regardless of whether it falls in the current range
                        nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        nextEntry = sstable.metadata.comparator.rowIndexEntrySerializer().deserialize(ifile, sstable.descriptor.version);
                        lastDeserialziedPos = (ifile instanceof PageAlignedReader) ? ((PageAlignedReader)ifile).getOffset() : ifile.getFilePointer();
                        lastDeserializedMark = ifile.mark();
                        readEnd = nextEntry.getPosition();
                    }

                    if (nextKey != null && !currentRange.contains(nextKey))
                    {
                        nextKey = null;
                        nextEntry = null;
                    }
                }

                if (dataRange == null || dataRange.selectsFullRowFor(currentKey.getKey()))
                {
                    dfile.seek(currentEntry.getPosition());
                    ByteBufferUtil.readWithShortLength(dfile); // key
                    long dataSize = readEnd - dfile.getFilePointer();
                    return new SSTableIdentityIterator(sstable, dfile, currentKey, dataSize);
                }

                return new LazyColumnIterator(currentKey, new IColumnIteratorFactory()
                {
                    public OnDiskAtomIterator create()
                    {
                        return dataRange.columnFilter(currentKey.getKey()).getSSTableColumnIterator(sstable, dfile, currentKey, currentEntry);
                    }
                });

            }
            catch (CorruptSSTableException | IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        private void maybeCloseCurrentIndexEntry()
        {
            if (currentEntry != null)
                currentEntry.close();
        }

        private void maybeCloseNextIndexEntry()
        {
            if (nextEntry != null)
                nextEntry.close();
        }

        @Override
        public void close() throws Exception
        {
            maybeCloseCurrentIndexEntry();
            maybeCloseNextIndexEntry();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "dfile=" + dfile +
               " ifile=" + ifile +
               " sstable=" + sstable +
               ")";
    }

    public static class EmptySSTableScanner implements ISSTableScanner
    {
        private final String filename;

        public EmptySSTableScanner(String filename)
        {
            this.filename = filename;
        }

        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public String getBackingFiles()
        {
            return filename;
        }

        public boolean hasNext()
        {
            return false;
        }

        public OnDiskAtomIterator next()
        {
            return null;
        }

        public void close() throws IOException { }

        public void remove() { }
    }


}
