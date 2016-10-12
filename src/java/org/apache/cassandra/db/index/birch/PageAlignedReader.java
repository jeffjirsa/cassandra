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

import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.index.birch.PageAlignedWriter.SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SUB_SEGMENT;
import static org.apache.cassandra.db.index.birch.PageAlignedWriter.SIZE_SEGMENT_HEADER_SERIALIZATION_OVERHAED;
import static org.apache.cassandra.db.index.birch.PageAlignedWriter.SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT;
import static org.apache.cassandra.db.index.birch.PageAlignedWriter.SEGMENT_ONLY_HAS_SINGLE_ALIGNED_SUBSEGMENT;
import static org.apache.cassandra.db.index.birch.PageAlignedWriter.SEGMENT_ONLY_HAS_SINGLE_NON_ALIGNED_SUBSEGMENT;

/**
 * Reads an aligned and segmented file. Documentation on the
 * the binary format used to serialize compatable files is available in the
 * {@link org.apache.cassandra.db.index.birch.PageAlignedWriter} docs.
 * <p>
 * Instances of this class are <b>not</b> thread safe.
 *
 * @see PageAlignedWriter
 */
public class PageAlignedReader implements FileDataInput, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(PageAlignedReader.class);

    private final File file;
    private final BufferingPageAlignedReader raf;

    private int totalSegments;
    private long serializedDescriptorsStartingOffset;
    private int relativeOffsetToSerializedAlignedSegmentObjects;
    private long maxOffset;
    private int pageAlignedChunkSize;

    private int currentSegmentIdx;
    private long currentSegmentOffset;
    private long currentSegmentUsableEndOffset;
    private long currentSegmentAlignedEndOffset;

    private short currentSubSegmentIdx;
    private short totalSubSegmentsInCurrentSegment;
    private long currentSubSegmentOffset;
    private long currentSubSegmentUsableEndOffset;
    private long currentSubSegmentAlignedEndOffset;
    private boolean currentSubSegmentIsPageAligned;

    private Exception closeException = null;
    private boolean isClosed = false;

    public PageAlignedReader(File file) throws IOException
    {
        this(file, false);
    }

    private PageAlignedReader(File file, boolean skipSetup) throws IOException
    {
        this.file = file;
        this.raf = new BufferingPageAlignedReader(file);
        if (!skipSetup)
            setup();
    }

    public static PageAlignedReader copy(PageAlignedReader src) throws IOException
    {
        PageAlignedReader newReader = new PageAlignedReader(src.file, true);
        newReader.totalSegments = src.totalSegments;
        newReader.serializedDescriptorsStartingOffset = src.serializedDescriptorsStartingOffset;
        newReader.relativeOffsetToSerializedAlignedSegmentObjects = src.relativeOffsetToSerializedAlignedSegmentObjects;
        newReader.maxOffset = src.maxOffset;
        newReader.pageAlignedChunkSize = src.pageAlignedChunkSize;

        newReader.currentSegmentIdx = src.currentSegmentIdx;
        newReader.currentSegmentOffset = src.currentSegmentOffset;
        newReader.currentSegmentUsableEndOffset = src.currentSegmentUsableEndOffset;
        newReader.currentSegmentAlignedEndOffset = src.currentSegmentAlignedEndOffset;

        newReader.currentSubSegmentIdx = src.currentSubSegmentIdx;
        newReader.totalSubSegmentsInCurrentSegment = src.totalSubSegmentsInCurrentSegment;
        newReader.currentSubSegmentOffset = src.currentSubSegmentOffset;
        newReader.currentSubSegmentUsableEndOffset = src.currentSubSegmentUsableEndOffset;
        newReader.currentSubSegmentAlignedEndOffset = src.currentSubSegmentAlignedEndOffset;
        newReader.currentSubSegmentIsPageAligned = src.currentSubSegmentIsPageAligned;

        newReader.seek(newReader.currentSubSegmentOffset);

        return newReader;
    }

    private void setup() throws IOException
    {
        seekInternal(raf.length() - Long.BYTES);
        this.serializedDescriptorsStartingOffset = readLong();
        seekInternal(serializedDescriptorsStartingOffset);
        this.totalSegments = readInt();
        this.relativeOffsetToSerializedAlignedSegmentObjects = readInt(); // relative starting offset of serialized segments
        this.maxOffset = readLong();
        this.pageAlignedChunkSize = readInt();

        raf.updateBufferSize(pageAlignedChunkSize);

        setCurrentBoundsForSegment(0, 0);

        seek(0);
    }

    private void setCurrentBoundsForSegment(AlignedSegment alignedSegment, int subSegmentIdx)
    {
        currentSegmentIdx = alignedSegment.idx;
        currentSegmentOffset = alignedSegment.offset;
        currentSegmentUsableEndOffset = alignedSegment.offset + alignedSegment.length;
        currentSegmentAlignedEndOffset = alignedSegment.offset + alignedSegment.alignedLength;

        AlignedSubSegment alignedSubSegment = alignedSegment.getSubSegments().get(subSegmentIdx);
        currentSubSegmentIdx = alignedSubSegment.idx;
        totalSubSegmentsInCurrentSegment = (short) alignedSegment.getSubSegments().size();
        currentSubSegmentOffset = alignedSubSegment.offset;
        currentSubSegmentUsableEndOffset = alignedSubSegment.offset + alignedSubSegment.length;
        currentSubSegmentAlignedEndOffset = alignedSubSegment.offset + alignedSubSegment.alignedLength;
        currentSubSegmentIsPageAligned = alignedSubSegment.isPageAligned;
    }

    private void setCurrentBoundsForSegment(int segmentIdx, int subSegmentIdx) throws IOException
    {
        AlignedSegment alignedSegment = getSegmentPointers(segmentIdx);
        setCurrentBoundsForSegment(alignedSegment, subSegmentIdx);
    }

    private AlignedSegment getSegmentPointers(int segmentIdx) throws IOException
    {
        long originalOffset = getOffset();

        // skip the fixed components (e.g. total segments, relative offset to variable length stuff, things we need in setup())
        // and then skip in SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT times the segment we want to get to the start
        // of the fixed length components serialized for that segment id
        seekInternal(serializedDescriptorsStartingOffset
                           + SIZE_SEGMENT_HEADER_SERIALIZATION_OVERHAED
                           + (SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT * segmentIdx));

        // the starting offset into the file for this segment
        long segmentOffset = readLong();
        // the total usable length of all segments (excluding any possible padding for alignment on page boundaries)
        long totalUsableLength = readLong();
        // the total length (including any possible padding) of a segment
        long totalAlignedLength = readLong();
        // the relative offset to start reading at to deserialize variable length parts for segment's sub-segments
        int relativeOffsetForThisSegmentsVariableLengthComponents = readInt();

        List<AlignedSubSegment> subSegments = new ArrayList<>();
        if (relativeOffsetForThisSegmentsVariableLengthComponents == SEGMENT_ONLY_HAS_SINGLE_ALIGNED_SUBSEGMENT
                || relativeOffsetForThisSegmentsVariableLengthComponents == SEGMENT_ONLY_HAS_SINGLE_NON_ALIGNED_SUBSEGMENT)
        {
            // we only have a single sub-segment in this segment, so use the "global" data deserialized
            // for the segment as the same offsets and length of the "sub-segment"
            boolean subSegmentIsAligned = relativeOffsetForThisSegmentsVariableLengthComponents == SEGMENT_ONLY_HAS_SINGLE_ALIGNED_SUBSEGMENT;
            subSegments.add(new AlignedSubSegment((short) 0, segmentOffset, totalUsableLength, totalAlignedLength, subSegmentIsAligned));
        }
        else
        {
            seekInternal(serializedDescriptorsStartingOffset
                     + relativeOffsetToSerializedAlignedSegmentObjects
                     + relativeOffsetForThisSegmentsVariableLengthComponents);

            short totalSubSegments = readShort();
            long startingOffsetForSubSegmentComponents = getOffset();
            for (int i = 0; i < totalSubSegments; i++)
            {
                seekInternal(startingOffsetForSubSegmentComponents
                         + (SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SUB_SEGMENT * i));

                // starting offset for this sub-segment
                long subSegmentOffset = readLong();
                // the length of data for this sub-segment (excluding any padding for page alignment)
                long subSegmentUsableLength = readLong();
                // the length of the data serialized for this sub-segment including any possible padding for page alignment
                long subSegmentAlignedLength = readLong();
                // if the sub-segment was serialized out as page aligned (thus possibly padded to an aligned boundry)
                boolean subSegmentIsPageAligned = readBoolean();
                subSegments.add(new AlignedSubSegment((short) i, subSegmentOffset, subSegmentUsableLength,
                                                      subSegmentAlignedLength, subSegmentIsPageAligned));
            }
        }

        // always return the offset for the file pointer on the raf back to the same position
        // it was in when the method was originally called
        seekInternal(originalOffset);

        return new AlignedSegment(segmentIdx, segmentOffset, totalUsableLength, totalAlignedLength, subSegments);
    }

    protected long getNextAlignedOffset(long offset)
    {
        // if offset <= alignTo, return 0
        // if next aligned offset > offset, return aligned offset - alignTo
        int alignTo = pageAlignedChunkSize;
        if (alignTo == 0 || offset == alignTo)
            return offset;
        if (offset <= alignTo)
            return 0;
        long alignedOffset = (offset + alignTo - 1) & ~(alignTo - 1);
        return (alignedOffset > offset) ? alignedOffset - alignTo : alignedOffset;
    }

    /**
     * @return total number of Aligned Segments encoded in this file
     */
    public int numberOfSegments()
    {
        return totalSegments;
    }

    /**
     * @return ttal number of aligned sub-segments in the current segment
     */
    public short numberOfSubSegments()
    {
        return totalSubSegmentsInCurrentSegment;
    }

    /**
     * @return a new instance of AlignedSegmentIterator
     */
    public AlignedSegmentIterator getSegmentIterator()
    {
        return new AlignedSegmentIterator();
    }

    public AlignedSegmentIterator getSegmentIterator(int startingSegment)
    {
        return new AlignedSegmentIterator(startingSegment);
    }

    /**
     * Returns an Iterator that will return all AlignedSegments in this PageAligned
     * file. Each returned AlignedSegment is lazily constructed as needed.
     */
    public class AlignedSegmentIterator extends AbstractIterator<AlignedSegment>
    {
        private int currentSegment;

        public AlignedSegmentIterator(int startingSegment)
        {
            this.currentSegment = startingSegment;
        }

        public AlignedSegmentIterator()
        {
            this.currentSegment = 0;
        }

        @Override
        public AlignedSegment computeNext()
        {
            if (currentSegment + 1 > totalSegments)
            {
                return endOfData();
            }

            try
            {
                return getSegmentPointers(currentSegment++);
            }
            catch (IOException e)
            {
                logger.error("Failed to deserialize aligned segment [idx {} of {}] from {}", currentSegment,
                             totalSegments, file.getAbsolutePath());
                throw new RuntimeException(e);
            }
        }
    }

    public void setSegment(int segmentId) throws IOException
    {
        setSegment(segmentId, 0);
    }

    public void setSegment(int segmentId, int subSegmentId) throws IOException
    {
        assert segmentId < totalSegments;

        setCurrentBoundsForSegment(segmentId, subSegmentId);
        seek(currentSubSegmentOffset);
    }

    public void nextSubSegment() throws IOException
    {
        setSegment(currentSegmentIdx, currentSubSegmentIdx + 1);
    }

    public boolean hasNextSubSegment()
    {
        return currentSubSegmentIdx + 1 < totalSubSegmentsInCurrentSegment;
    }

    public boolean hasNextSegment()
    {
        return currentSegmentIdx + 1 < totalSegments;
    }

    public boolean hasPreviousSegment()
    {
        return currentSegmentIdx - 1 >= 0;
    }

    public void previousSegment() throws IOException
    {
        setSegment(currentSegmentIdx - 1);
    }

    public void nextSegment() throws IOException
    {
        setSegment(currentSegmentIdx + 1);
    }

    public long getOffset()
    {
        return raf.getPosition();
    }

    public String getPath()
    {
        return file.getAbsolutePath();
    }

    public boolean isEOF() throws IOException
    {
        boolean isOutOfSegments = currentSegmentIdx + 1 >= totalSegments;
        boolean isOutOfSubSegments = currentSubSegmentIdx + 1 >= totalSubSegmentsInCurrentSegment;
        boolean currentSubSegmentExausted = getOffset() >= currentSubSegmentUsableEndOffset;
        return isOutOfSegments && isOutOfSubSegments && currentSubSegmentExausted;
    }

    public boolean isCurrentSegmentExausted() throws IOException
    {
        return getOffset() >= currentSegmentUsableEndOffset;
    }

    public boolean isPositionInsideCurrentSegment(long pos)
    {
        return pos >= currentSegmentOffset && pos <= currentSegmentAlignedEndOffset;
    }

    public boolean isPositionInsideCurrentSubSegment(long pos)
    {
        return pos >= currentSubSegmentOffset && pos <= currentSubSegmentAlignedEndOffset;
    }

    public long bytesRemaining() throws IOException
    {
        return currentSegmentAlignedEndOffset - getOffset();
    }

    private void findAndSetCurrentSegmentAndSubSegmentForPosition(long pos) throws IOException
    {
        // given input position not within the current segment, so
        // binary search for the segment that contains the input position
        int segmentIdxContainingPos = findIdxForPosition(pos);
        setCurrentSubSegmentForPositionWithKnownSegmentIdx(pos, segmentIdxContainingPos);
    }

    private void setCurrentSubSegmentForPositionWithKnownSegmentIdx(long pos, int segmentIdx) throws IOException
    {
        AlignedSegment alignedSegment = getSegmentPointers(segmentIdx);

        int subSegmentIdx = 0;
        if (alignedSegment.getSubSegments().size() > 1)
        {
            for (AlignedSubSegment subSegment : alignedSegment.getSubSegments())
            {
                if (subSegment.isPositionInSubSegment(pos))
                {
                    subSegmentIdx = subSegment.idx;
                    break;
                }
            }
        }

        setCurrentBoundsForSegment(alignedSegment, subSegmentIdx);
    }

    /**
     * Scans thru all segments and subsegments and does a bounds check for the provided position.
     * When a segment + sub segment combination is found that contains the given position, the
     * pointers for both the current segment and subsegment are updated to that segment.
     *
     * Every effort should be made to avoid this check as it's obviously not optimal, but if
     * PageAlignedReader instances are reused, it might be required that we use it to re-prepare
     * the PageAlignedReader instance. A good example of where this is required is in
     * {@link SSTableScanner#seekToCurrentRangeStart()}
     *
     * @param pos absolute position in the data file to find the segment and subsegment bounds for
     * @throws IOException thrown if an exception is encountered while mapping the found region
     */
    public void findAndSetSegmentAndSubSegmentCurrentForPosition(long pos) throws IOException
    {
        assert pos >= 0 && pos <= maxOffset;

        if (pos < currentSegmentOffset || pos > currentSegmentUsableEndOffset)
        {
            findAndSetCurrentSegmentAndSubSegmentForPosition(pos);
            seek(pos);
        }

        if (pos > currentSubSegmentUsableEndOffset || pos < currentSubSegmentOffset
                                                        && (pos >= currentSegmentOffset && pos <= currentSegmentUsableEndOffset))
        {
            setCurrentSubSegmentForPositionWithKnownSegmentIdx(pos, currentSegmentIdx);
            seek(pos);
        }
    }

    /**
     * Performs a Binary Search across the fixed length encoded components
     * of the segments on disk.
     *
     * @param pos the file position to determine the index of the aligned segment
     *            that contains it
     * @return the index of the AlignedSegment that contains the provided position
     */
    public int findIdxForPosition(long pos) throws IOException
    {
        if (pos > maxOffset)
            throw new IOException("Invalid Position Provided. pos " + pos + " > maxOffset " + maxOffset);

        // optimization to avoid doing any of this work at all for pos 0
        if (pos == 0)
            return 0;

        long markedPos = getOffset();

        int retIdx = -1;

        int start = 0;
        int end = totalSegments - 1; // binary search is zero-indexed
        int middle = (end - start) / 2;

        while (start <= end)
        {
            // skip to the start of the fixed serialized components for the current 'middle' element
            seekInternal(serializedDescriptorsStartingOffset
                     + SIZE_SEGMENT_HEADER_SERIALIZATION_OVERHAED
                     + (middle * SIZE_FIXED_LENGTH_COMPONENTS_SINGLE_SEGMENT));

            long offset = readLong();

            int cmp = Long.compare(offset, pos);
            if (cmp == 0)
            {
                return middle;
            }

            if (cmp < 0)
            {
                retIdx = middle;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        seek(markedPos);

        return retIdx;
    }

    private void seekInternal(long pos) throws IOException
    {
        raf.seek(pos);
    }

    public void seek(long pos) throws IOException
    {
        if(!(pos >= 0 && pos >= currentSubSegmentOffset && pos <= currentSubSegmentAlignedEndOffset))
            logger.warn(String.format("About to fail assert in seek(%s) - pos = %s , sub segment offset %s , aligned end offset %s, reader %s", pos, pos, currentSubSegmentOffset, currentSubSegmentAlignedEndOffset, this));
        assert pos >= 0 && pos >= currentSubSegmentOffset && pos <= currentSubSegmentAlignedEndOffset;

        raf.seek(pos);
    }

    public void seekToStartOfCurrentSubSegment() throws IOException
    {
        seek(currentSubSegmentOffset);
    }

    public void seekToEndOfCurrentSubSegment() throws IOException
    {
        seek(currentSubSegmentAlignedEndOffset);
    }

    public void seekToEndOfCurrentSegment() throws IOException
    {
        seek(currentSegmentAlignedEndOffset);
    }

    public int getCurrentSegmentIdx()
    {
        return currentSegmentIdx;
    }

    public short getCurrentSubSegmentIdx()
    {
        return currentSubSegmentIdx;
    }

    public boolean isCurrentSubSegmentPageAligned()
    {
        return currentSubSegmentIsPageAligned;
    }

    public long getCurrentSubSegmentAlignedEndOffset()
    {
        return currentSubSegmentAlignedEndOffset;
    }

    public int getPageAlignedChunkSize()
    {
        return pageAlignedChunkSize;
    }

    public FileMark mark()
    {
        return new PageAlignedFileMark(getOffset(), currentSegmentIdx, currentSubSegmentIdx);
    }

    public void reset(FileMark mark) throws IOException
    {
        PageAlignedFileMark alignedMark = (PageAlignedFileMark) mark;
        if (!isPositionInsideCurrentSubSegment(alignedMark.pointer))
            setCurrentBoundsForSegment(alignedMark.currentSegmentIdx, alignedMark.currentSubSegmentIdx);
        seek(alignedMark.pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        return 0;
    }

    public long getFilePointer()
    {
        return getOffset();
    }

    /**
     * Read length bytes from current file position
     *
     * @param length length of the bytes to read
     * @return buffer with bytes read
     * @throws IOException if any I/O operation failed
     */
    public ByteBuffer readBytes(int length) throws IOException
    {
        byte[] buf = new byte[length];
        try
        {
            raf.read(buf);
        }
        catch (BufferUnderflowException | BufferOverflowException e)
        {
            logger.error("Something bad happened reading from the current mmapped buffer with length {}.. current state of this PageAlignedReader: [{}]", length, toString(), e);
            throw e;
        }
        return ByteBuffer.wrap(buf);
    }

    public Exception getCloseException()
    {
        return closeException;
    }

    @Override
    public void close() throws IOException
    {
        if (isClosed)
            return;

        // tmp kjkj: capture stack when close() is called so
        // when a caller incorrectly operates on this same instance after
        // it has already been closed we can know who closed it originally..
        try
        {
            throw new Exception();
        }
        catch (Exception e)
        {
            closeException = e;
        }

        isClosed = true;

        raf.close();
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        raf.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        raf.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        long offsetAfterSkip = getOffset() + n;
        assert offsetAfterSkip <= currentSegmentAlignedEndOffset;
        seek(offsetAfterSkip);
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return ((int) raf.readByte()) == 1;
    }

    @Override
    public byte readByte() throws IOException
    {
        return raf.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() throws IOException
    {
        return raf.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        int ch1 = readByte() & 0xFF;
        int ch2 = readByte() & 0xFF;
        if ((ch1 | ch2) < 0)
            throw new IOException("Failed to read unsigned short as deserialized value is bogus/negative");
        return (ch1 << 8) + (ch2);
    }

    @Override
    public char readChar() throws IOException
    {
        return raf.readChar();
    }

    @Override
    public int readInt() throws IOException
    {
        return raf.readInt();
    }

    @Override
    public long readLong() throws IOException
    {
        return raf.readLong();
    }

    @Override
    public float readFloat() throws IOException
    {
        return raf.readFloat();
    }

    @Override
    public double readDouble() throws IOException
    {
        return raf.readDouble();
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public String toString()
    {
        return "PageAlignedReader[file="+this.file+",totalSegments="+this.totalSegments+",pageAlignedChunkSize="+this.pageAlignedChunkSize+"]";
    }
}
