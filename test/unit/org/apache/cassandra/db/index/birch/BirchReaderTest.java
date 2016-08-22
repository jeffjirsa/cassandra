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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.SimpleCType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.io.sstable.IndexInfo;

/**
 * Tests the correctness of BirchReader. Many related/additional tests
 * can be found in PageAlignedReaderTest, PageAlignedWriterTest,
 * and BirchWriterTest.
 */
public class BirchReaderTest
{

    @Test
    public void testForwardsIteration() throws Exception
    {
        testIteration(3000, false);
    }

    @Test
    public void testReversedIteration() throws Exception
    {
        testIteration(3000, true);
    }

    private void testIteration(int targetTreeSize, boolean reversed) throws Exception
    {
        File tmpFile = File.createTempFile(UUID.randomUUID().toString(), "btree");

        PageAlignedWriter writer = new PageAlignedWriter(tmpFile);

        CType type = new SimpleCType(TimeUUIDType.instance);

        Iterator<TreeSerializable> timeUUIDIterator = new TimeUUIDTreeSerializableIterator(targetTreeSize, 100);
        BirchWriter birchWriter = new BirchWriter.Builder<>(timeUUIDIterator, BirchWriter.SerializerType.INDEXINFO, type).build();

        writer.startNewSegment();
        writer.startNewSubSegment(birchWriter.getCacheLineSize());
        birchWriter.serialize(writer);
        writer.finalizeCurrentSubSegment();
        writer.finalizeCurrentSegment();
        writer.close();

        PageAlignedReader reader = new PageAlignedReader(tmpFile);
        reader.setSegment(0);
        BirchReader<IndexInfo> birchReader = new BirchReader<>(reader);

        // IndexInfo doesn't override equals, so put the bytes in the set
        // and check that for the samples in this case...
        Set<ByteBuffer> sampleNames = new HashSet<>();
        for (IndexInfo sample : ((TimeUUIDTreeSerializableIterator) timeUUIDIterator).getSamples())
        {
            sampleNames.add(sample.getFirstName());
        }

        BirchReader.BirchIterator iterator = birchReader.getIterator(type, reversed);

        int iteratorCount = 0;
        int sampleMatches = 0;
        while (iterator.hasNext())
        {
            IndexInfo next = (IndexInfo) iterator.next();
            if (sampleNames.contains(next.getFirstName()))
                sampleMatches++;
            iteratorCount++;
        }

        Assert.assertEquals(birchReader.getElementCount(), iteratorCount);
        Assert.assertEquals(targetTreeSize, iteratorCount);
        Assert.assertEquals(sampleNames.size(), sampleMatches);
    }
}
