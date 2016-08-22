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

import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.apache.cassandra.utils.CLibrary;

/**
 * Extends RandomAccessFile by buffering a certain amount of bytes onto the heap
 * and returning bytes from the buffer as necessary instead of directly from the
 * backing RAF.
 * <p>
 * The idea behind this class is that reading a page aligned block from the disk
 * in one swoop into the heap should be about as expensive as reading a single byte,
 * short, int, or long from the same segment. The first PageAlignedReader/PageAlignedWriter
 * design wrote out data to disk on aligned boundaries with the idea that the disk will
 * be more efficient at retriving that unit of work. Originally, I lazily mmap'ed each
 * aligned chunk (4kb by default) as needed. Upon benchmarking, I found the overhead of
 * mmap can be very significant (90+ms to mmap a single 4kb region of a file in the 99th percentile).
 * I then tried simply reading directly from the backing RAF. While this provided pretty good
 * performance (as most of the index files should be in the page cache anyways) I still found
 * some unpredictablity with system calls in the 99.9th percentile. The idea behind this
 * class is to implement something very simple that allows the caller to transparently operate on a file
 * just like a vanilla {@link java.io.RandomAccessFile} without worrying about the fact that there is a
 * backing buffer. As {@link org.apache.cassandra.db.index.birch.BirchReader} segments are serialized
 * out as 4KB sized (by default) nodes this means we should be able to operate inside multiple elements
 * in a single Birch node (for example) with a single system call.
 * <p>
 * Instances of this class are <b>not</b> thread safe, just like a regular {@link java.io.RandomAccessFile}
 * @see PageAlignedWriter
 * @see PageAlignedReader
 */
public class BufferingPageAlignedReader extends InputStream implements DataInput, AutoCloseable
{
    private static final int DEFAULT_BUFFER_SIZE = 1 << 12; // 4kb
    private static final int MINIMUM_BUFFER_SIZE = 2 << 9; // 1kb
    private static final int MAXIMUM_BUFFER_SIZE = 2 << 15; // 64kb

    private final File file;
    private final long length;
    private final FileChannel channel;

    private boolean isClosed;

    private ByteBuffer buffer;
    private int bufferSize;

    private int currentBufferPosition;
    private int currentBufferUsableLength;
    private long position;
    private long currentBufferStartOffset;
    private long currentBufferEndOffset;

    public BufferingPageAlignedReader(File file, int bufferSize) throws IOException
    {
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.file = file;
        this.bufferSize = bufferSize;
        this.length = file.length();
        CLibrary.tryDisableReadAhead(CLibrary.getfd(channel), 0, length);
    }

    public BufferingPageAlignedReader(File file) throws IOException
    {
        this(file, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Set's the size of the backing buffer to use. Calling this method will not
     * result in immediate resizing of the buffer, instead the next time the buffer
     * needs to be remapped, the buffer will be resized at that time to match the
     * provided bufferSize
     *
     * @param bufferSize the size of the backing buffer to use going forward. Must be
     *                   >= MINIMUM_BUFFER_SIZE and <= MAXIMUM_BUFFER_SIZE
     */
    public void updateBufferSize(int bufferSize)
    {
        assert bufferSize >= MINIMUM_BUFFER_SIZE && bufferSize <= MAXIMUM_BUFFER_SIZE;
        this.bufferSize = bufferSize;
    }

    public long length()
    {
        return length;
    }

    // don't make this public, this is only for seeking WITHIN the current mapped segment
    protected void seekInternal(int pos)
    {
        position = pos;
    }

    /**
     * @param offset the offset in the file that is actually wanted
     * @return the nearest aligned offset in the file that will contain the given offset
     */
    private long getNextAlignedOffset(long offset)
    {
        // if offset <= alignTo, return 0
        // if next aligned offset > offset, return aligned offset - alignTo
        int alignTo = bufferSize;
        if (alignTo == 0 || offset == alignTo)
            return offset;
        if (offset <= alignTo)
            return 0;
        long alignedOffset = (offset + alignTo - 1) & ~(alignTo - 1);
        return (alignedOffset > offset) ? alignedOffset - alignTo : alignedOffset;
    }

    /**
     * @param pos the absolute position in the file to check if it is within the current buffer's boundaries
     * @return if the absolute input position is within the current boundaries contained in the current buffer
     */
    private boolean isPositionWithinCurrentBufferBoundaries(long pos)
    {
        return pos >= currentBufferStartOffset && pos < currentBufferEndOffset;
    }

    /**
     * Checks if the current position is within the start and stop boundaries covered by the current
     * backing buffer. If the position is not, we find the nearest aligned offset and copy up to the
     * current buffer's length from the backing RAF into the current buffer and update the pointers and
     * offsets.
     * <p>
     * If the current position is covered by the bounds mapped for the current buffer, this method is a no-op.
     * @throws IOException an io error occured while reading or seeking within the file
     */
    private void maybeUpdateBuffer() throws IOException
    {
        if (isEOF())
            return;

        if (!isPositionWithinCurrentBufferBoundaries(position) || buffer == null)
        {
            if (buffer == null || buffer.capacity() != bufferSize)
            {
                this.buffer = ByteBuffer.wrap(new byte[bufferSize]);
            }

            // we always want to read from the nearest aligned boundary (as determined by the buffer size,
            // which should match the alignment boundaries the PageAligned file was written out with)
            long nextAlignedOffset = getNextAlignedOffset(position);
            channel.position(nextAlignedOffset);

            // if we have less bytes than the buffer size available from the nearest aligned offset to the end of the file
            // we only want to map the remaining bytes available from the nearest aligned offset
            int lengthToRead = (nextAlignedOffset + bufferSize > length) ? (int) (length - nextAlignedOffset) : bufferSize;

            this.buffer.position(0);
            this.buffer.limit(lengthToRead);

            // transfer the available bytes into the buffer
            int actuallyRead = 0;
            while (actuallyRead < lengthToRead)
            {
                int read = channel.read(buffer);
                if (read < 0)
                    throw new EOFException(); // should never happen, lengthToRead incorrectly calculated?
                channel.position(nextAlignedOffset + read);
                actuallyRead += read;
            }

            // update the relative position inside the new current buffer to match the current absolute position
            currentBufferPosition = (int) (position - nextAlignedOffset);
            // as we re-use the same backing buffer, we need to track the number of bytes to use the byte[] buffer's length
            currentBufferUsableLength = lengthToRead;

            // update the absolute boundaries for the start end end offsets that the newly set buffer covers
            currentBufferStartOffset = nextAlignedOffset;
            currentBufferEndOffset = nextAlignedOffset + lengthToRead;
        }
    }

    /**
     * @return the number of usable bytes remaining from the current position in the current buffer
     */
    private int bufferRemainingBytes()
    {
        return currentBufferUsableLength - currentBufferPosition;
    }

    public void seek(long newPosition) throws IOException
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition > length)
            throw new IllegalArgumentException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, file.getAbsolutePath(), length));

        if (isClosed)
            throw new IOException(String.format("Unable to seek to position %d as this %s instance has " +
                                                "previously been closed", newPosition, this.getClass().getName()));

        // we want to update the position first as we use the position inside maybeUpdateBuffer()
        // to determine if we need to swap out the current buffer
        position = newPosition;

        maybeUpdateBuffer();

        // maybeUpdateBuffer will only update the currentBufferPosition if the current buffer was swapped.
        // we need to update the current buffer here to handle the case where seek was called for a position
        // that fits within the buffer's offsets that were already previously mapped.
        // for example (assuming a file aligned on 4kb boundaries when written): if seek was called with position
        // 16484 on a file with a length of 16492, we would read in 108 bytes to the buffer (16384 would be the
        // nearest 4kb aligned offset). The buffer is then able to return data for any absolute positions
        // requested from 16384 => 16492 (the file length). The relative offset for the buffer for absolute
        // position 16484 would be 100. If seek was then called again for absolute position 16386, the current
        // buffer already covers it (16384 => 16492) but if other reads have occured in the mean time the current
        // buffer's position may have been incremented within the current segment. So, we need to reset the relative
        // position on every call of seek to ensure we skip to the appropriate relative length inside the current buffer.
        currentBufferPosition = (int) (position - currentBufferStartOffset);
    }

    public int read() throws IOException
    {
        if (isEOF())
            return -1;

        maybeUpdateBuffer();

        int ret = buffer.get(currentBufferPosition++) & 0xff;
        position++;

        return ret;
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        if (isClosed || position + len > length)
        {
            throw new EOFException();
        }

        int currentDestOffset = off;
        int bytesRemainingToRead = len;
        while (bytesRemainingToRead > 0)
        {
            maybeUpdateBuffer();

            int bytesToRead = (bytesRemainingToRead <= bufferRemainingBytes()) ? bytesRemainingToRead : bufferRemainingBytes();
            System.arraycopy(buffer.array(), currentBufferPosition, b, currentDestOffset, bytesToRead);
            currentDestOffset += bytesToRead;
            position += bytesToRead;
            currentBufferPosition += bytesToRead;
            bytesRemainingToRead -= bytesToRead;
        }

        return len;
    }

    public void readFully(byte[] b) throws IOException
    {
        this.read(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        this.read(b, off, len);
    }

    public int skipBytes(int n) throws IOException
    {
        assert n >= 0 : "skipping negative bytes is illegal: " + n;
        if (n == 0)
            return 0;
        seek(position + n);
        return n;
    }

    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public String readUTF() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public boolean readBoolean() throws IOException
    {
        return read() == 1;
    }

    public byte readByte() throws IOException
    {
        return (byte) read();
    }

    public int readUnsignedByte() throws IOException
    {
        return (byte) read();
    }

    public short readShort() throws IOException
    {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    public int readUnsignedShort() throws IOException
    {
        int ch1 = readByte() & 0xFF;
        int ch2 = readByte() & 0xFF;
        if ((ch1 | ch2) < 0)
            throw new IOException("Failed to read unsigned short as deserialized value is bogus/negative");
        return (ch1 << 8) + (ch2);
    }

    public char readChar() throws IOException
    {
        return (char) read();
    }

    public int readInt() throws IOException
    {
        int ch1 = this.read();
        int ch2 = this.read();
        int ch3 = this.read();
        int ch4 = this.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readLong() throws IOException
    {
        return ((long)(readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    public float readFloat() throws IOException
    {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException
    {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * @return the 'absolute' current position of the file
     */
    public long getPosition()
    {
        return position;
    }

    public boolean isEOF()
    {
        return isClosed || position == length;
    }

    @Override
    public void close() throws IOException
    {
        if (isClosed)
            return;

        this.buffer = null;
        channel.close();
        isClosed = true;
    }
}
