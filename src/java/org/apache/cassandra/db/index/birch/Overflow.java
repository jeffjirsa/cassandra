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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used while creating a new {@link org.apache.cassandra.db.index.birch.BirchWriter}
 * tree to keep keys added to he tree that cannot fit within a single Birch
 * {@link org.apache.cassandra.db.index.birch.Node}.
 *
 * The overflow page is now internally padded and chunked along page boundaries,
 * but is still padded to the nearest page boundary.
 * <p>
 * While reading a BirchWriter tree {@link org.apache.cassandra.db.index.birch.BirchReader},
 * we will read directly from the overflow component as required meaning these reads will
 * be less optimial, most likely more expensive, and likely to slow down overall read latencies.
 *
 * @see BirchWriter
 */
public class Overflow implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(Overflow.class);

    private static final String TMP_OVERFLOW_FILE_EXTENSION = "overflowtmp";
    private static final int MAX_SIZE_ON_HEAP_BUFFER = 1 << 23;

    private final String indexInProgressPath;
    private File tmpOverflowFile;

    // do not allocate buffers by default as the "normal" case should be no overflow page
    // we will use an on-heap ByteBuffer for the Overflow while it remains less than 1 << 23 bytes (~8MB)
    // once it exceeds ~8MB we will switch to a tmp file backing buffer to avoid GC pressure during compaction
    private ByteBuffer overflowBuf = null;
    private byte[] buffer = null;

    private RandomAccessFile overflowFileBuf;

    public Overflow(String path)
    {
        this.indexInProgressPath = path;
    }

    private void createOverflowFileBuf() throws IOException
    {
        this.tmpOverflowFile = new File(String.format("%s.%s", indexInProgressPath, TMP_OVERFLOW_FILE_EXTENSION));
        this.overflowFileBuf = new RandomAccessFile(tmpOverflowFile, "rw");
    }

    private void maybeAllocateReusableScratchBuffer()
    {
        if (buffer == null)
            buffer = new byte[1 << 12];
    }

    private int transferOnHeapBufferToFile(DataOutput out) throws IOException
    {
        maybeAllocateReusableScratchBuffer();

        overflowBuf.position(0);
        int totalBytesWritten = 0;
        int bytesRemainingToWrite = overflowBuf.limit();
        while (bytesRemainingToWrite > 0)
        {
            int lengthToWrite = (bytesRemainingToWrite < buffer.length) ? bytesRemainingToWrite : buffer.length;
            overflowBuf.get(buffer, 0, lengthToWrite);
            out.write(buffer, 0, lengthToWrite);
            bytesRemainingToWrite -= lengthToWrite;
            totalBytesWritten += lengthToWrite;
        }

        return totalBytesWritten;
    }

    private static ByteBuffer resizeOverflowBufferPreservingContents(ByteBuffer srcBuf, int newSize)
    {
        ByteBuffer resizedBuffer = ByteBuffer.allocate(newSize);
        srcBuf.position(0);
        resizedBuffer.put(srcBuf);
        resizedBuffer.position(srcBuf.position());
        return resizedBuffer;
    }

    private void prepareBuffersForInternalAdd(ByteBuffer elm) throws IOException
    {
        if (overflowBuf == null && overflowFileBuf == null)
        {
            if ((elm.remaining() * 2) > MAX_SIZE_ON_HEAP_BUFFER)
            {
                createOverflowFileBuf();
            }
            else
            {
                overflowBuf = ByteBuffer.allocate((elm.remaining() + Integer.BYTES) * 2);
            }
        }
        else if (overflowBuf != null && overflowFileBuf == null)
        {
            if (overflowBuf.remaining() < elm.remaining() + Integer.BYTES)
            {
                // we don't have enough space in the on-heap overflow buffer to add
                // this overflow component. we need to resize, or if resizing will exceed
                // our maximum target on-heap size, create the tmp file, transfer the
                // bytes already written to the on-heap buffer, and then use that going forward
                int idealNewCapacity = ((overflowBuf.capacity() * 3) / 2) + 1;
                while ((elm.remaining() + Integer.BYTES) > (overflowBuf.capacity() - overflowBuf.position()))
                {
                    if (idealNewCapacity > MAX_SIZE_ON_HEAP_BUFFER)
                        break;

                    idealNewCapacity += ((idealNewCapacity * 3) / 2) + 1;
                }

                if (idealNewCapacity > MAX_SIZE_ON_HEAP_BUFFER)
                {
                    // the resized capacity of the on-heap buffer is going to exceed the max size
                    // we're willing to spend without going to a file backed buffer.
                    // create the tmp file buffer, copy the contents of the existing heap buffer
                    // and then use the file buffer going forward for the remainder of the lifespan
                    // of this Overflow object
                    createOverflowFileBuf();

                    transferOnHeapBufferToFile(overflowFileBuf);

                    overflowBuf = null;
                }
                else
                {
                    // we can resize the on-heap buffer and still use that
                    this.overflowBuf = resizeOverflowBufferPreservingContents(overflowBuf, idealNewCapacity);
                }
            }
        }
    }

    public long add(ByteBuffer elm) throws IOException
    {
        maybeAllocateReusableScratchBuffer();
        prepareBuffersForInternalAdd(elm);

        if (overflowBuf != null)
        {
            // we want to transfer the bytes of the element that couldn't fit in the leaf
            // so this will be from the current position to the limit
            int bytesRemaining = elm.limit() - elm.position();
            // serialize the length of the bytes we're about to write so we can deserialize on the other side
            overflowBuf.putInt(bytesRemaining);
            while (bytesRemaining > 0)
            {
                int bytesToTransfer = (bytesRemaining > buffer.length) ? buffer.length : bytesRemaining;
                elm.get(buffer, 0, bytesToTransfer);
                overflowBuf.put(buffer, 0, bytesToTransfer);
                bytesRemaining -= bytesToTransfer;
            }

            return overflowBuf.position();
        }
        else
        {
            // we want to transfer the bytes of the element that couldn't fit in the leaf
            // so this will be from the current position to the limit
            int bytesRemaining = elm.limit() - elm.position();
            // serialize the length of the bytes we're about to write so we can deserialize on the other side
            overflowFileBuf.writeInt(bytesRemaining);
            while (bytesRemaining > 0)
            {
                int bytesToTransfer = (bytesRemaining > buffer.length) ? buffer.length : bytesRemaining;
                elm.get(buffer, 0, bytesToTransfer);
                overflowFileBuf.write(buffer, 0, bytesRemaining);
                bytesRemaining -= bytesToTransfer;
            }

            return overflowFileBuf.getFilePointer();
        }
    }

    public long serialize(PageAlignedWriter writer) throws IOException
    {
        if (overflowBuf != null)
        {
            return transferOnHeapBufferToFile(writer);
        }
        else if (overflowFileBuf != null)
        {
            overflowFileBuf.getFD().sync();

            long totalBytesWritten = 0;
            long bytesRemaining = overflowFileBuf.length();
            overflowFileBuf.seek(0);
            while (bytesRemaining > 0)
            {
                int bytesToTransfer = (bytesRemaining > buffer.length) ? buffer.length : (int) bytesRemaining;
                overflowFileBuf.read(buffer, 0, bytesToTransfer);
                writer.write(buffer, 0, bytesToTransfer);
                bytesRemaining -= bytesToTransfer;
                totalBytesWritten += bytesToTransfer;
            }
            return totalBytesWritten;
        }

        return 0;
    }

    @Override
    public void close() throws IOException
    {
        if (overflowFileBuf != null)
        {
            overflowFileBuf.close();
            if (!tmpOverflowFile.delete())
                logger.error("Failed to delete temporary index Overflow buffer {}", tmpOverflowFile.getAbsolutePath());

            overflowFileBuf = null;
        }

        overflowBuf = null;
        buffer = null;
    }
}
