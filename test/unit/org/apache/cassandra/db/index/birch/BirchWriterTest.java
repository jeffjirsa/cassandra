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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.SimpleCType;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.IndexInfo;

public class BirchWriterTest
{
    private static final Logger logger = LoggerFactory.getLogger(BirchWriterTest.class);

    private static final Random RANDOM = new Random();

    @Test
    public void wickedLargeTreeWithoutOverflow() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(100000, 5000);
        BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void multipleWickedLargeTreesWithoutOverflow() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int i = 0; i < 3; i++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
                ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
                writer.writeShort(key.capacity());
                writer.write(key);
                writer.finalizeCurrentSubSegment();

                Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(50000 * (i + 1), 5000);
                List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

                BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();
                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.setSegment(1);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.setSegment(2);
            Assert.assertTrue(reader.hasNextSubSegment());
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void wickedLargeTreeWithOverflow() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);

        PageAlignedWriter writer = new PageAlignedWriter(tmpFile);

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(85000, 5000);
        List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

        BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type)
                                  .maxKeyLengthWithoutOverflow((short) 10) // set the max key length super small so we test overflow logic without needing large test input keys
                                  .build();

        writer.startNewSegment();
        writer.startNewSubSegment(birchWriter.getCacheLineSize());
        birchWriter.serialize(writer);
        writer.finalizeCurrentSubSegment();
        writer.finalizeCurrentSegment();
        writer.close();

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);
            Assert.assertEquals(85000, birchReader.getElementCount());

            for (IndexInfo sample : samples)
            {
                IndexInfo indexInfo = birchReader.search(sample.getFirstNameAsComposite(), type, false);
                Assert.assertEquals(sample.offset, indexInfo.offset);
            }
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void searchForExactMatch() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(10000, 300);
        List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewNonPageAlignedSubSegment();
            String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
            ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
            writer.writeShort(key.capacity());
            writer.write(key);
            writer.finalizeCurrentSubSegment();

            BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();

            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.nextSubSegment();

            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);
            Assert.assertEquals(10000, birchReader.getElementCount());
            IndexInfo res = birchReader.search(samples.get(3).getFirstNameAsComposite(), type, false);
            Assert.assertTrue(type.compare(res.getFirstNameAsComposite(), samples.get(3).getFirstNameAsComposite()) == 0);
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void multipleTreesAndIterateAllElementsForwards() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int i = 0; i < 3; i++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
                ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
                writer.writeShort(key.capacity());
                writer.write(key);
                writer.finalizeCurrentSubSegment();

                Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(50000 * (i + 1), 300);
                BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();

                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(1);
            reader.nextSubSegment();
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);
            Iterator<IndexInfo> iterator = birchReader.getIterator(type, false);
            int iteratorCount = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = iterator.next();
                iteratorCount++;
            }

            Assert.assertEquals(50000 * 2, iteratorCount);
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void multipleTreesAndIterateAllElementsReversed() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);

        List<IndexInfo> expected = null;
        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            for (int i = 0; i < 3; i++)
            {
                writer.startNewSegment();
                writer.startNewNonPageAlignedSubSegment();
                String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
                ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
                writer.writeShort(key.capacity());
                writer.write(key);
                writer.finalizeCurrentSubSegment();

                Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(10000 * (i + 1), (i == 0) ? -1 : 0);
                if (i == 0)
                    expected = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();

                BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();

                writer.startNewSubSegment(birchWriter.getCacheLineSize());
                birchWriter.serialize(writer);
                writer.finalizeCurrentSubSegment();
                writer.finalizeCurrentSegment();
            }
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            Assert.assertTrue(reader.hasNextSubSegment());
            reader.nextSubSegment();
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);
            ListIterator<IndexInfo> expectedIterator = expected.listIterator(expected.size());
            Iterator<IndexInfo> iterator = birchReader.getIterator(type, true);
            int iteratorCount = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = iterator.next();
                IndexInfo expectedIndexInfo = expectedIterator.previous();

                Assert.assertTrue(type.compare(expectedIndexInfo.getFirstNameAsComposite(), next.getFirstNameAsComposite()) == 0);
                Assert.assertEquals(expectedIndexInfo.offset, next.offset);
                Assert.assertEquals(expectedIndexInfo.width, next.width);
                iteratorCount++;
            }

            Assert.assertEquals(10000, iteratorCount);
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void absolutelyWickedMassiveTree() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(TimeUUIDType.instance);
        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(3000000, 1000000);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();
            writer.startNewSegment();
            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);

            List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();
            for (IndexInfo sample : samples)
            {
                IndexInfo indexInfo = birchReader.search(sample.getFirstNameAsComposite(), type, false);
                logger.debug("birchWriter returned {}:{}:{}.... looking for {}:{}:{}",
                            TimeUUIDType.instance.getString(indexInfo.getFirstName()), indexInfo.offset, indexInfo.width,
                            TimeUUIDType.instance.getString(sample.getFirstName()), sample.offset, sample.width);
                Assert.assertEquals(sample.offset, indexInfo.offset);
            }
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    @Ignore
    public void tmpKjPerf() throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");
        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);

            List<Composite> lookup = new ArrayList<>();
            List<String> gen = new ArrayList<>();
            gen.add("2efe952e-f7ac-11e5-9a30-0d228f97559b");
            gen.add("240f82fa-f7ac-11e5-9a30-0d228f97559b");
            gen.add("194a3f6f-f7ac-11e5-9a30-0d228f97559b");
            gen.add("0ebbc354-f7ac-11e5-9a30-0d228f97559b");
            gen.add("03cd7470-f7ac-11e5-9a30-0d228f97559b");
            gen.add("f85cfcc5-f7ab-11e5-9a30-0d228f97559b");
            gen.add("edca3b61-f7ab-11e5-9a30-0d228f97559b");
            gen.add("e3464696-f7ab-11e5-9a30-0d228f97559b");
            gen.add("d8a246ab-f7ab-11e5-9a30-0d228f97559b");
            gen.add("cdf03d77-f7ab-11e5-9a30-0d228f97559b");
            gen.add("c27bce2c-f7ab-11e5-9a30-0d228f97559b");
            gen.add("8b32d233-f7ab-11e5-9a30-0d228f97559b");

            for (String timeStr : gen)
            {
                UUID uuid = UUID.fromString(timeStr);
                SimpleCType.SimpleCBuilder builder = new SimpleCType.SimpleCBuilder(new SimpleDenseCellNameType(TimeUUIDType.instance));
                builder.add(uuid);
                lookup.add(builder.build());
            }

            // quick warm-up iterations..
            for (int i = 0; i < 5; i++)
            {
                for (Composite toLookup : lookup)
                {
                    IndexInfo person = birchReader.search(toLookup, new SimpleCType(TimeUUIDType.instance), false);
                }
            }

            CType type = new SimpleCType(TimeUUIDType.instance);

            com.codahale.metrics.Timer timer = new com.codahale.metrics.Timer();
            for (int i = 0; i < 1000; i++)
            {
                for (Composite toLookup : lookup)
                {
                    final com.codahale.metrics.Timer.Context timerContext = timer.time();
                    IndexInfo indexInfo = birchReader.search(toLookup, type, false);
                    logger.info("birch returned {}:{}:{}", TimeUUIDType.instance.getString(indexInfo.getFirstName()), indexInfo.offset, indexInfo.width);
                    timerContext.stop();
                }
            }

            logger.error("Timer ({} iterations) => 95th: {} 99th: {} 99.9th: {} mean: {} median: {}", timer.getCount(),
                         timer.getSnapshot().get95thPercentile(), timer.getSnapshot().get99thPercentile(),
                         timer.getSnapshot().get999thPercentile(), timer.getSnapshot().getMean(), timer.getSnapshot().getMedian());
        }

        Assert.assertTrue(tmpFile.delete());
    }

    @Test
    public void simpleTreeIterateStartingFromSearchOffset() throws IOException
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        CType type = new SimpleCType(UTF8Type.instance);

        int sampleRate = 5000;
        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(50000, sampleRate);

        try (PageAlignedWriter writer = new PageAlignedWriter(tmpFile))
        {
            writer.startNewSegment();
            writer.startNewNonPageAlignedSubSegment();
            String keyStr = generateRandomWord(RANDOM.nextInt(50) + 3);
            ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes()).duplicate();
            writer.writeShort(key.capacity());
            writer.write(key);
            writer.finalizeCurrentSubSegment();

            BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();

            writer.startNewSubSegment(birchWriter.getCacheLineSize());
            birchWriter.serialize(writer);
            writer.finalizeCurrentSubSegment();
            writer.finalizeCurrentSegment();
        }

        try (PageAlignedReader reader = new PageAlignedReader(tmpFile))
        {
            reader.setSegment(0);
            reader.nextSubSegment();

            BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);
            Assert.assertEquals(50000, birchReader.getElementCount());
            List<IndexInfo> samples = ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples();
            Iterator<IndexInfo> iterator = birchReader.getIterator(samples.get(1).getFirstNameAsComposite(), type, false);
            int iteratorCount = 0;
            while (iterator.hasNext())
            {
                IndexInfo next = iterator.next();
                iteratorCount++;
            }
            Assert.assertEquals(50000 - (sampleRate - 1), iteratorCount);
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
