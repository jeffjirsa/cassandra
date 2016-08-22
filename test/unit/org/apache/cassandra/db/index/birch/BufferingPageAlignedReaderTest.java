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
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests reading across bounderies and other edge conditions that might occur
 * while reading values out of a RAF thru a BufferingPageAlignedReader
 */
public class BufferingPageAlignedReaderTest
{
    private static final Random RANDOM = new Random();

    @Test
    public void testReadingAcrossMultipleBufferBounderies() throws Exception
    {
        String randomWord = generateRandomWord((4096 * 3) + 2);
        byte[] randomWordBytes = randomWord.getBytes();
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "bfile");
        try
        {
            RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
            raf.write(randomWord.getBytes());
            raf.writeInt(1);
            raf.writeInt(7);
            raf.writeLong(1234);
            raf.close();

            try (BufferingPageAlignedReader bufferingReader = new BufferingPageAlignedReader(tmpFile, 4096))
            {
                byte[] deserializedRandomWord = new byte[randomWordBytes.length];
                bufferingReader.read(deserializedRandomWord);

                Assert.assertArrayEquals(randomWordBytes, deserializedRandomWord);

                Assert.assertEquals(1, bufferingReader.readInt());
                Assert.assertEquals(7, bufferingReader.readInt());
                Assert.assertEquals(1234, bufferingReader.readLong());
            }
        }
        finally
        {
            Assert.assertTrue(tmpFile.delete());
        }
    }

    @Test
    public void testSeekingMultipleTimesWithinTheSameBuffer() throws Exception
    {
        int randomWordLength = 1024;
        List<byte[]> generatedWords = new ArrayList<>();
        for (int i = 0; i < 6; i++)
        {
            String randomWord = generateRandomWord(randomWordLength);
            generatedWords.add(randomWord.getBytes());
        }

        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "bfile");
        try
        {
            RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
            for (byte[] generatedWord : generatedWords)
            {
                raf.write(generatedWord);
            }
            raf.close();

            try (BufferingPageAlignedReader bufferingReader = new BufferingPageAlignedReader(tmpFile, 2 << 11))
            {
                bufferingReader.seek(randomWordLength);
                byte[] deserializedRandomWord = new byte[randomWordLength];
                bufferingReader.read(deserializedRandomWord);
                Assert.assertArrayEquals(generatedWords.get(1), deserializedRandomWord);

                // seek to the 3rd generated word within the same 4kb buffer (skipping the 2nd)
                bufferingReader.seek(randomWordLength * 3);
                bufferingReader.read(deserializedRandomWord);
                Assert.assertArrayEquals(generatedWords.get(3), deserializedRandomWord);

                // seek to the 5th generated word which should exist in a new buffer
                bufferingReader.seek(randomWordLength * 5);
                bufferingReader.read(deserializedRandomWord);
                Assert.assertArrayEquals(generatedWords.get(5), deserializedRandomWord);

                // go back to the 3rd generated word which should also read in a new buffer
                bufferingReader.seek(randomWordLength * 3);
                bufferingReader.read(deserializedRandomWord);
                Assert.assertArrayEquals(generatedWords.get(3), deserializedRandomWord);
            }
        }
        finally
        {
            Assert.assertTrue(tmpFile.delete());
        }
    }

    private static String generateRandomWord(int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append((char) (RANDOM.nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
