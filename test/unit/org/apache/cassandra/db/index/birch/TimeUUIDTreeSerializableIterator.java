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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.SimpleCType;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A AbstractIterator implementation that returns TreeSerializable objects
 * for use with Birch unit tests.
 *
 * Originally, I used word lists to test the accuracy of the tree, however,
 * I started running into issues when testing very large trees. The size of the
 * temporary test lists with the words from the word lists caused lots of GC pressure
 * and made the tests not very effective.
 *
 * The idea here is to use TimeUUIDs which are already sorted as they are generated,
 * so they can be created on the fly, and also track samples at a given rate, so
 * you can then get an arbitrary number of values you know where inserted into the
 * tree to validate your unit test.
 */
public class TimeUUIDTreeSerializableIterator extends AbstractIterator<TreeSerializable>
{
    private static final Logger logger = LoggerFactory.getLogger(TimeUUIDTreeSerializableIterator.class);

    private final long size;
    private final int trackSampleRate;
    private long pos;
    private UUID previousUUID = null;
    private final List<IndexInfo> samples;

    /**
     * @param size the number of TimeUUID elements to iterate thru
     * @param trackSampleRate the sample rate at which to keep TimeUUIDs returned from the
     *                        iterator to use for external validation. A trackSampleRate of -1
     *                        will track all elements returned by the iterator as samples. A
     *                        trackSampleRate of 0 will keep 0 samples.
     */
    public TimeUUIDTreeSerializableIterator(long size, int trackSampleRate)
    {
        this.size = size;
        this.trackSampleRate = trackSampleRate;
        this.samples = new ArrayList<>();
    }

    public TreeSerializable computeNext()
    {
        if (pos++ >= size)
        {
            return endOfData();
        }

        UUID uuid = UUIDGen.getTimeUUID();
        // if we iterate quickly enough we can generate the same time uuid as the
        // clock hasn't moved forwards... cheap hacky way to avoid duplicates
        while (previousUUID != null && uuid.equals(previousUUID))
        {
            uuid = UUIDGen.getTimeUUID();
        }
        previousUUID = uuid;

        CType type = new SimpleDenseCellNameType(TimeUUIDType.instance);
        SimpleCType.SimpleCBuilder builder = new SimpleCType.SimpleCBuilder(type);
        builder.add(uuid);
        Composite name = builder.build();
        IndexInfo indexInfo = new IndexInfo(name.toByteBuffer(), pos, pos, type);

        if (trackSampleRate != 0 && (pos == 1 || trackSampleRate == -1 || pos % trackSampleRate == 0))
        {
            logger.debug("adding to samples... {}:{}:{}", TimeUUIDType.instance.getString(indexInfo.getFirstName()),
                        indexInfo.offset, indexInfo.width);
            samples.add(indexInfo);
        }

        return indexInfo;
    }

    public List<IndexInfo> getSamples()
    {
        return samples;
    }
}
