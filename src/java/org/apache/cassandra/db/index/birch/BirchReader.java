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

import com.google.common.collect.AbstractIterator;

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.BirchMetrics;
import org.apache.cassandra.utils.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.cassandra.db.index.birch.BirchWriter.SERIALIZERS;

/**
 * Provides logic to both search and iterate through a serialized Birch tree.
 *
 * Refer to {@link org.apache.cassandra.db.index.birch.BirchWriter} for documentation
 * on the Birch file format. A BirchReader is backed by a {@link org.apache.cassandra.db.index.birch.PageAlignedReader}
 * which handles the underlying managment of memory mapping and reading from disk in
 * a cache-friendly/aligned way.
 * <p>
 * This class is <b>*not*</b> thread safe.
 *
 * @see BirchWriter
 */
public class BirchReader<T> implements Closeable
{
    private PageAlignedReader reader;
    private final Descriptor descriptor;
    private final BSerializer<T> serializer;

    public BirchReader(PageAlignedReader reader) throws IOException
    {
        this.reader = reader;

        assert reader.isCurrentSubSegmentPageAligned();

        reader.seek(reader.getCurrentSubSegmentAlignedEndOffset() - reader.getPageAlignedChunkSize());
        this.descriptor = Descriptor.deserialize(reader);
        reader.seekToStartOfCurrentSubSegment();
        this.serializer = SERIALIZERS.get(descriptor.getSerializerType());
    }

    public void unsafeReplaceReader(PageAlignedReader reader)
    {
        this.reader = reader;
    }

    /**
     * @return the total number of elements available in this tree
     */
    public int getElementCount()
    {
        return descriptor.getElementCount();
    }

    /**
     * @param type an instance of CType to use
     * @param reversed if the iterator should iterate thru elements in either forwards or reversed direction
     * @return an instance of BirchIterator
     * @throws IOException thrown if a deserialization or IO error is encountered while creating the iterator
     */
    public BirchIterator getIterator(CType type, boolean reversed) throws IOException
    {
        return new BirchIterator(type, reversed);
    }

    /**
     * @param searchKey start the iteration of the iterator from this provided key
     * @param type an instance of CType to use
     * @param reversed if the iterator should iterate thru elements in either forwards or reversed direction
     * @return an instance of BirchIterator
     * @throws IOException thrown if a deserialization or IO error is encountered while creating the iterator
     */
    public BirchIterator getIterator(Composite searchKey, CType type, boolean reversed) throws IOException
    {
        return new BirchIterator(searchKey, type, reversed);
    }

    /**
     * Returns the key and raw file offset of the element in the tree at idx. If the
     * key has an overflow component, the key returned will be a fully reassembled ByteBuffer
     * containing both the part of the key in the tree and the remaining bytes deserialized
     * from the overflow page.
     *
     * @param idx the index of the element in the tree to return
     * @return a KeyAndOffsetPtr object that contains a ByteBuffer of the key for that
     *         element and the raw file offset where that key was found
     * @throws IOException thrown if an IOException is encountered while deserializing the key
     */
    private KeyAndOffsetPtr getKeyAndOffsetPtr(int idx) throws IOException
    {
        PageAlignedFileMark segmentStartMark = (PageAlignedFileMark) reader.mark();

        short entries = reader.readShort();

        // check if element has overflow length encoded after key
        int byteForIdxInOverflowField = Byte.BYTES * (int) (idx / 8.0);
        reader.seek(segmentStartMark.pointer + Short.BYTES
                    + ((entries * Short.BYTES) + Short.BYTES) + (entries * Long.BYTES) + byteForIdxInOverflowField);
        byte hasOverflowByte = reader.readByte();
        boolean hasOverflow = (hasOverflowByte >> (idx % 8) & 1) == 1;

        // Skip to internal offsets section for this element to find where to start reading the key.
        // Get this elements offset and elm + 1's offset. We calculate the length to read as
        // (elm + 1 offset) - (elm offset)
        reader.seek(segmentStartMark.pointer + Short.BYTES + (idx * Short.BYTES));
        short offsetInNodeToKey = reader.readShort();
        short offsetOfNextKey = reader.readShort();
        int lengthOfKey = offsetOfNextKey - offsetInNodeToKey;

        // skip into this segment past encoded elements and encoded key offsets. Then skip idx number of Longs
        // into the encoded offsets to get the offset for this element
        reader.seek(segmentStartMark.pointer + Short.BYTES + ((entries * Short.BYTES) + Short.BYTES) + (idx * Long.BYTES));
        long ptrOffset = reader.readLong();

        // skip to the starting offset of the key
        reader.seek(segmentStartMark.pointer + offsetInNodeToKey);

        ByteBuffer key = getKey(lengthOfKey, hasOverflow);

        // the reader makes the assumptions that previous operations will clean
        // up after themselves and will always reset the segment back to the start
        reader.reset(segmentStartMark);

        return new KeyAndOffsetPtr(key, ptrOffset);
    }

    /**
     * @param idx the index of the element in the tree to deserialize and return
     * @param type a CType instance to use
     * @return the object (of type T) found at the given index
     * @throws IOException thrown if an IOException is encountered while deserializing the element
     */
    private T getElement(int idx, CType type) throws IOException
    {
        PageAlignedFileMark segmentStartMark = (PageAlignedFileMark) reader.mark();

        short entries = reader.readShort();

        long startingOffsetsPosition = reader.getOffset();
        long valuesOffset = startingOffsetsPosition + ((entries + 1) * Short.BYTES);

        // check if this element has an overflow component
        int byteForIdxInOverflowField = Byte.BYTES * (int) (idx / 8.0);
        reader.seek(valuesOffset + (entries * serializer.serializedValueSize()) + byteForIdxInOverflowField);
        byte hasOverflowByte = reader.readByte();
        boolean hasOverflow = (hasOverflowByte >> (idx % 8) & 1) == 1;

        // calculate length of encoded key from the encoded offset of (elm + 1) - elm
        reader.seek(startingOffsetsPosition + (idx * Short.BYTES));
        short offsetInNodeToKey = reader.readShort();
        short offsetOfNextKey = reader.readShort();
        int lengthOfKey = offsetOfNextKey - offsetInNodeToKey;

        reader.seek(segmentStartMark.pointer + offsetInNodeToKey);

        ByteBuffer key = getKey(lengthOfKey, hasOverflow);

        reader.seek(valuesOffset + (idx * serializer.serializedValueSize()));
        T obj = serializer.deserializeValue(key, type, reader);

        // always reset back to offset we started with as reader makes assumptions
        // we'll always be at the start of a segment
        reader.reset(segmentStartMark);

        return obj;
    }

    /**
     * @param lengthOfKey the length of the bytes of the key encoded in the Birch node itself
     * @param hasOverflow if the key has an Overflow component
     * @return a ByteBuffer with the full contents of the key deserialized from the Index
     * @throws IOException an io error occured while reading the key from the Index file
     */
    private ByteBuffer getKey(int lengthOfKey, boolean hasOverflow) throws IOException
    {
        ByteBuffer key = (hasOverflow)
                         ? getKeyWithOverflow(lengthOfKey)
                         : getKeyWithoutOverflow(lengthOfKey);

        // always ensure we set the position() of the key we are returning to 0
        // so regardless of what happens, the caller can always assume it doesn't
        // need to reset the position before using it.
        key.position(0);

        BirchMetrics.totalBytesReadPerKey.update(key.limit());

        return key;
    }

    /**
     * @param lengthOfKey the number of bytes to read from the backing Index for this key
     * @return a ByteBuffer of the key containing the bytes read from the backing Index file
     * @throws IOException an io error occured while reading the key from the Index file
     */
    private ByteBuffer getKeyWithoutOverflow(int lengthOfKey) throws IOException
    {
        ByteBuffer key = reader.readBytes(lengthOfKey);
        return key;
    }

    /**
     *
     * @param lengthOfKey the length of the bytes of the key to read from the component inside
     *                    the Birch node itself
     * @return a reassembled single ByteBuffer containing the entire key with the bytes from
     *         both the Birch node and the overflow bytes as necessary
     * @throws IOException an io error occured while reading the key from the Index file
     */
    private ByteBuffer getKeyWithOverflow(int lengthOfKey) throws IOException
    {
        // if the key has an overflow component, remove one Long worth of bytes
        // from key length encoded in the birch node itself as calculated size
        // will include encoded overflow offset (if element has overflow component)
        lengthOfKey = lengthOfKey - Long.BYTES;

        BirchMetrics.readsRequiringOverflow.mark();

        FileMark markBeforeOverflowSeek = reader.mark();

        // we have to skip over the key to get to the offset in the overflow page
        // we'll skip back and read it later so we can allocate a single buffer
        // instead of allocating one to get the bytes from the node we're skipping
        // initially here, one for the overflow bytes, and a final merged one for the two
        reader.skipBytes(lengthOfKey);

        // read relative overflow offset encoded at end of key
        long offsetInOverflowPage = reader.readLong();
        // skip to offset in overflow and get key's overflow bytes. then we
        // create a single merged byte buffer with the merged results of the
        // key bytes from the tree and it's overflow bytes
        reader.seek(descriptor.getOverflowPageOffset() + offsetInOverflowPage);
        int lengthToRead = reader.readInt();
        BirchMetrics.additionalBytesReadFromOverflow.update(lengthToRead);

        // create one byte[] that fits the entire key (both the part of the
        // key that fit in the birch node itself and the remaining overflow bits)
        byte[] reconstructedKeyBuf = new byte[lengthOfKey + lengthToRead];
        reader.readFully(reconstructedKeyBuf, lengthOfKey, lengthToRead);

        // seek the reader back to the offset in the file where the bytes for the
        // key that fit inside the birch leaf are
        reader.reset(markBeforeOverflowSeek);

        // read the first (and remaining) bytes for the key from the birch node
        // to fully reassembly the key
        reader.readFully(reconstructedKeyBuf, 0, lengthOfKey);
        return ByteBuffer.wrap(reconstructedKeyBuf);
    }

    /**
     * Returns a object found in the tree by performing a binary search within a leaf node
     *
     * @param searchKey the Composite to binary search the leaf for
     * @param type the CType instance to use
     * @param reversed if searchKey contains an empty ByteBuffer, reversed is used to determine if
     *                 we should return the first (or last) element
     * @return a Pair, where the left is the object to return and right is the
     *         index of element in the tree being returned
     * @throws IOException thrown if an IOException is encountered while searching the leaf
     */
    private Pair<T, Integer> binarySearchLeaf(Composite searchKey, CType type, boolean reversed) throws IOException
    {
        PageAlignedFileMark nodeStartingMark = (PageAlignedFileMark) reader.mark();

        if (searchKey.isEmpty())
        {
            if (reversed)
            {
                short entries = reader.readShort();
                Pair<T, Integer> ret = Pair.create(getElement((int) entries - 1, type), (int) entries - 1);
                reader.reset(nodeStartingMark);
                return ret;
            }
            else
            {
                return Pair.create(getElement(0, type), 0);
            }
        }

        short entries = reader.readShort();

        T ret = null;
        int retIdx = -1;

        int start = 0;
        int end = entries - 1; // binary search is zero-indexed
        int middle = (end - start) / 2;

        while (start <= end)
        {
            reader.reset(nodeStartingMark);

            T elm = getElement(middle, type);

            ByteBuffer key = ((TreeSerializable) elm).serializedKey(type);

            int cmp = type.compare(type.fromByteBuffer(key), searchKey);
            if (cmp == 0)
            {
                return Pair.create(elm, middle);
            }

            if (cmp < 0)
            {
                ret = elm;
                retIdx = middle;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        reader.reset(nodeStartingMark);

        return Pair.create(ret, retIdx);
    }

    /**
     * Perform a binary search over an inner-node to return the offset
     * in the file for the leaf node that should contain searchKey
     *
     * @param searchKey the Composite to binary search for. If empty, this method will return the
     *                  first (or last) offset depending on the value of reversed
     * @param type an instance of CType
     * @param reversed if searchKey contains an empty ByteBuffer, reversed is used to determine if
     *                 we should return the offset of the first or last leaf
     * @return
     * @throws IOException
     */
    private long binarySearchNode(Composite searchKey, CType type, boolean reversed) throws IOException
    {
        assert searchKey != null;

        PageAlignedFileMark nodeStartingMark = (PageAlignedFileMark) reader.mark();

        if (searchKey.isEmpty())
        {
            if (reversed)
            {
                short entries = reader.readShort();
                KeyAndOffsetPtr keyAndOffsetPtr = getKeyAndOffsetPtr(entries - 1);
                reader.reset(nodeStartingMark);
                return keyAndOffsetPtr.offsetPtr;
            }
            else
            {
                return getKeyAndOffsetPtr(0).offsetPtr;
            }
        }

        short entries = reader.readShort();

        long retOffset = -1;

        int start = 0;
        int end = entries - 1; // binary search is zero-indexed
        int middle = (start + end) / 2;

        while (start <= end)
        {
            reader.reset(nodeStartingMark);

            KeyAndOffsetPtr keyAndOffsetPtr = getKeyAndOffsetPtr(middle);

            if (entries == 1)
            {
                retOffset = keyAndOffsetPtr.offsetPtr;
                break;
            }

            int cmp = type.compare(type.fromByteBuffer(keyAndOffsetPtr.key), searchKey);
            if (cmp == 0)
            {
                retOffset = keyAndOffsetPtr.offsetPtr;
                break;
            }

            if (cmp < 0)
            {
                retOffset = keyAndOffsetPtr.offsetPtr;
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
            middle = (start + end) / 2;
        }

        return retOffset;
    }

    /**
     * Search the Birch tree for a specific key (and return) an instance of T
     * with the value deserialized from the matching element in the tree.
     *
     * @param searchKey the Composite to search for
     * @param type an instnace of CType to use for the Composite
     * @param reversed if the search logic should iterate on the tree in forwards or reversed order
     *
     * @return an instance of T matching the searchKey
     * @throws IOException thrown if an IOException is encountered while searching the tree
     */
    public T search(Composite searchKey, CType type, boolean reversed) throws IOException
    {
        TimerContext timerContext = BirchMetrics.totalTimeSpentPerSearch.time();
        try
        {
            long offset = descriptor.getRootOffset();

            while (offset >= descriptor.getFirstNodeOffset())
            {
                reader.seek(offset);
                offset = binarySearchNode(searchKey, type, reversed);
                if (offset == -1)
                {
                    // todo: null is lame, use Optional instead?
                    return null;
                }
            }

            reader.seek(offset); // go to leaf node that we will return a result from

            Pair<T, Integer> res = binarySearchLeaf(searchKey, type, reversed);
            return res.left;
        }
        finally
        {
            timerContext.stop();
        }
    }

    private class KeyAndOffsetPtr
    {
        private final ByteBuffer key;
        private final long offsetPtr;

        public KeyAndOffsetPtr(ByteBuffer key, long offsetPtr)
        {
            this.key = key;
            this.offsetPtr = offsetPtr;
        }
    }

    /**
     * An Iterator implementation that can iterate either forwards or
     * reversed thru all elements, and optionally start the iteration
     * from a given provided start element.
     */
    public class BirchIterator extends AbstractIterator<T>
    {
        private final CType type;
        private final boolean reversed;

        private final long totalSizeOfLeafs;
        private final int totalLeafPages;

        private int currentPage;
        private int currentElmIdx;

        /**
         * Returns a new instance of BirchIterator for iterating either
         * forwards or reversed thru all elements in a Birch Tree, starting
         * the iteration from a specific element in the tree.
         *
         * If reversed, the first element returned will be the very last
         * element. If not-reversed, the first element returned will be the
         * first element in the tree.
         *
         * @param searchKey the Composite to start the iterator's iteration from
         * @param type the CType instance to use for the Composite elements in the tree
         * @param reversed if the tree should iterate over elements forwards or reversed
         * @throws IOException thrown if a IOException is encountered while creating the iterator
         */
        public BirchIterator(Composite searchKey, CType type, boolean reversed) throws IOException
        {
            this.type = type;
            this.reversed = reversed;
            this.totalSizeOfLeafs = descriptor.getFirstNodeOffset() - descriptor.getFirstLeafOffset();
            this.totalLeafPages = (int) (totalSizeOfLeafs / descriptor.getAlignedPageSize());

            if (searchKey != null && !searchKey.isEmpty())
            {
                // find the element closest to the search key and start the iteration from there
                long offset = descriptor.getRootOffset();

                while (offset >= descriptor.getFirstNodeOffset())
                {
                    reader.seek(offset);
                    offset = binarySearchNode(searchKey, type, reversed);
                }

                if (offset == -1)
                {
                    // search key wasn't found, start at either the first or last element
                    // depending on if the iterator is in reversed mode or not
                    initializeIteratorToDefaultStart();
                }
                else
                {
                    this.currentPage = (int) (offset - descriptor.getFirstLeafOffset()) / descriptor.getAlignedPageSize();

                    // go to leaf...
                    reader.seek(offset);

                    int binarySearchRes = binarySearchLeaf(searchKey, type, reversed).right;
                    this.currentElmIdx = (binarySearchRes < 0) ? 0 : binarySearchRes;
                }
            }
            else
            {
                initializeIteratorToDefaultStart();
            }
        }

        private void initializeIteratorToDefaultStart() throws IOException
        {
            // traverse and return all elements in the tree
            if (reversed)
            {
                this.currentPage = totalLeafPages;
                // as we are in reversed mode, go to the last leaf page
                reader.seek(descriptor.getFirstLeafOffset() + ((currentPage - 1) * (descriptor.getAlignedPageSize())));
                PageAlignedFileMark currentPageStart = (PageAlignedFileMark) reader.mark();
                short numElements = reader.readShort();
                this.currentElmIdx = (numElements == 1) ? 0 : numElements - 1;
                reader.reset(currentPageStart);
            }
            else
            {
                this.currentPage = 0;
                this.currentElmIdx = 0;
            }
        }

        /**
         * Returns a new instance of BirchIterator for iterating either
         * forwards or reversed thru all elements in a Birch Tree.
         *
         * If reversed, the first element returned will be the very last
         * element. If not-reversed, the first element returned will be the
         * first element in the tree.
         *
         * @param type the CType instance
         * @param reversed if the iterator should iterate forwards or backwards
         * @throws IOException thrown if an IOException is encountered while iterating the tree
         */
        public BirchIterator(CType type, boolean reversed) throws IOException
        {
            this(null, type, reversed);
        }

        public T computeNext()
        {
            try
            {
                if (reversed)
                {
                    int newCurrentPage = (currentPage == 0) ? 0 : currentPage - 1;
                    reader.seek(descriptor.getFirstLeafOffset() + (newCurrentPage * (descriptor.getAlignedPageSize())));
                }
                else
                {
                    reader.seek(descriptor.getFirstLeafOffset() + (currentPage * (descriptor.getAlignedPageSize())));
                }

                PageAlignedFileMark leafOffsetStart = (PageAlignedFileMark) reader.mark();

                short numElements = reader.readShort();

                if (!reversed && currentElmIdx + 1 > numElements)
                {
                    // we have iterated over all elements in the current page
                    // check if we have any other pages to consume from next
                    if (currentPage + 1 >= totalLeafPages)
                    {
                        // we have exhausted all elements in all pages
                        return endOfData();
                    }
                    else
                    {
                        currentPage++;
                        currentElmIdx = 0;
                        reader.seek(descriptor.getFirstLeafOffset() + (currentPage * (descriptor.getAlignedPageSize())));
                        leafOffsetStart = (PageAlignedFileMark) reader.mark();
                    }
                }
                else if (reversed && currentElmIdx < 0)
                {
                    // we have iterated over all elements in the current page
                    // check if we have any other pages to consume from next
                    if (currentPage - 1 <= 0)
                    {
                        // we have exhausted all elements in all pages
                        return endOfData();
                    }
                    else
                    {
                        currentPage--;
                        reader.seek(descriptor.getFirstLeafOffset() + ((currentPage - 1) * (descriptor.getAlignedPageSize())));
                        leafOffsetStart = (PageAlignedFileMark) reader.mark();
                        numElements = reader.readShort();
                        currentElmIdx = numElements - 1;
                    }
                }

                reader.seek(leafOffsetStart.pointer);
                T next = getElement(currentElmIdx, type);

                if (reversed)
                    currentElmIdx--;
                else
                    currentElmIdx++;

                return next;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(reader);
    }
}
