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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;

public class PageAlignedWriterTest
{
    public static final int DEFAULT_PAGE_ALIGNMENT_BOUNDARY = 4096;

    private static final Random RANDOM = new Random();

    @Test
    public void simpleSingleSegment() throws Exception
    {
        writeAndReadPageAlignedFile(1);
    }

    @Test
    public void simpleMultipleSegments() throws IOException
    {
        writeAndReadPageAlignedFile(4);
    }

    private static void writeAndReadPageAlignedFile(int numSegments) throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        String randomLongWord = generateRandomWord((1 << 18) - 5);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int j = 0; j < numSegments; j++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                writer.writeInt(randomLongWord.length());
                ByteBuffer randomLongWordBytes = ByteBuffer.wrap(randomLongWord.getBytes());
                writer.write(randomLongWordBytes);
                writer.finalizeCurrentSubSegment();

                writer.startNewSubSegment(DEFAULT_PAGE_ALIGNMENT_BOUNDARY);
                for (int k = 0; k < 10; k++)
                {
                    byte[] buf = new byte[4096];
                    for (int i = 0; i < 4092; i++)
                    {
                        buf[i] = 'a';
                    }
                    for (int i = 4092; i < 4096; i++)
                    {
                        buf[i] = 'x';
                    }
                    writer.write(buf, 0, buf.length);
                }
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            for (int i = 0; i < reader.numberOfSegments(); i++)
            {
                reader.setSegment(i, 0);
                int encodedWordLength = reader.readInt();
                ByteBuffer encodedWord = reader.readBytes(encodedWordLength);
                ByteBuffer randomLongWordBytes = ByteBuffer.wrap(randomLongWord.getBytes());
                Assert.assertTrue(UTF8Type.instance.compare(encodedWord, randomLongWordBytes) == 0);

                reader.setSegment(i, 1);
                char c = (char) reader.readByte();
                Assert.assertEquals('a', c);
                reader.seekToStartOfCurrentSubSegment();
                reader.skipBytes(4);
                char c2 = (char) reader.readByte();
                Assert.assertEquals('a', c2);
                reader.seekToStartOfCurrentSubSegment();
                reader.skipBytes((4096 * 2) + 24);
                char c3 = (char) reader.readByte();
                Assert.assertEquals('a', c3);
                reader.seekToStartOfCurrentSubSegment();
                reader.skipBytes(4096 - 2);
                char c4 = (char) reader.readByte();
                Assert.assertEquals('x', c4);
            }
        }

        Assert.assertTrue(tmpFile.delete());
    }

    private static String generateRandomWord(int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length * 5; i++)
        {
            sb.append((char) (RANDOM.nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
