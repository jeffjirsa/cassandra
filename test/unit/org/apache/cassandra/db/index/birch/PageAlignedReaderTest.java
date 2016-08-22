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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.FileMark;

public class PageAlignedReaderTest
{

    @Test
    public void calculateRelativeMmappedOffsets() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");
        try
        {
            createPageAlignedWriter(tmpFile);

            try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
            {
                reader.setSegment(0);

                Assert.assertEquals(0, reader.getNextAlignedOffset(0));
                Assert.assertNotSame(4096, reader.getNextAlignedOffset(0));
                Assert.assertEquals(0, reader.getNextAlignedOffset(2));
                Assert.assertEquals(0, reader.getNextAlignedOffset(4095));
                Assert.assertEquals(4096, reader.getNextAlignedOffset(4096));
                Assert.assertEquals(4096, reader.getNextAlignedOffset(4097));
                Assert.assertEquals(4096, reader.getNextAlignedOffset((4096 * 2) - 1));
                Assert.assertEquals((4096 * 2), reader.getNextAlignedOffset((4096 * 2)));
                Assert.assertEquals((4096 * 2), reader.getNextAlignedOffset((4096 * 2) + 1));
                Assert.assertEquals((4096 * 3), reader.getNextAlignedOffset((4096 * 3)));
                Assert.assertEquals((4096 * 3), reader.getNextAlignedOffset((4096 * 3) + 1));
            }
        }
        finally
        {
            Assert.assertTrue(tmpFile.delete());
        }
    }


    /**
     * Attempts to get a segment for a bogus offset (a.k.a. too big)
     */
    @Test(expected= IOException.class)
    public void getSegmentForBogusOffset() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");
        try
        {
            createPageAlignedWriter(tmpFile);

            try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
            {
                reader.findIdxForPosition(tmpFile.length() + 10);
            }
        }
        finally
        {
            Assert.assertTrue(tmpFile.delete());
        }
    }

    /**
     * Gets the cooresponding segment for a valid offset
     */
    @Test
    public void getSegmentForValidOffset() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");
        try
        {
            createPageAlignedWriter(tmpFile);

            try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
            {
                int segment1 = reader.findIdxForPosition(0);
                Assert.assertEquals(0, segment1);

                int segment2 = reader.findIdxForPosition((4096 * 2) + 2);
                Assert.assertEquals(2, segment2);
            }
        }
        finally
        {
            Assert.assertTrue(tmpFile.delete());
        }
    }

    @Test
    public void testMarkAndReset() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");
        try
        {
            createPageAlignedWriter(tmpFile);

            try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
            {
                reader.setSegment(0);

                FileMark mark1 = reader.mark();
                long mark1Offset = reader.getOffset();
                reader.readInt();
                Assert.assertNotSame(mark1Offset, reader.getOffset());
                reader.readInt();
                FileMark mark2 = reader.mark();
                long mark2Offset = reader.getOffset();
                reader.reset(mark1);
                Assert.assertSame(mark1Offset, reader.getOffset());
                reader.reset(mark2);
                Assert.assertSame(mark2Offset, reader.getOffset());
            }
        }
        finally
        {
            Assert.assertTrue(tmpFile.delete());
        }
    }

    private static void createPageAlignedWriter(File file) throws IOException
    {
        try (PageAlignedWriter writer = new PageAlignedWriter(file))
        {
            for (int i = 0; i < 5; i++)
            {
                writer.startNewSegment();
                writer.startNewSubSegment(4096);
                writer.writeInt(i);
                writer.writeInt(i);
                writer.writeInt(i);
                writer.writeInt(i);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }
    }
}
