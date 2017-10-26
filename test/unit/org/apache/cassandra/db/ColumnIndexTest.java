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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnIndexTest extends CQLTester
{
    private static final List<AbstractType<?>> clusterTypes = Collections.<AbstractType<?>>singletonList(LongType.instance);
    private static final ClusteringComparator comp = new ClusteringComparator(clusterTypes);
    private static ClusteringPrefix cn(long l)
    {
        return Util.clustering(comp, l);
    }

    @Test
    public void testListDownsampling()
    {
        List<IndexHelper.IndexInfo> columnIndex = new ArrayList<>();

        CFMetaData cfMeta = CFMetaData.compile("CREATE TABLE pipe.dev_null (pk bigint, ck bigint, val text, PRIMARY KEY(pk, ck))", "foo");

        DeletionTime deletionInfo = new DeletionTime(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds());

        int lastIndexEnd = 0;
        for(int i = 0; i < 101; i++)
        {
            columnIndex.add(new IndexHelper.IndexInfo(cn(5L * i), cn(5L * i + 5), lastIndexEnd, 10, deletionInfo));
            lastIndexEnd += 10;
        }
        Assert.assertEquals(101, columnIndex.size());

        // 101/2 is not evenly divisible, the first entry won't be merged
        ColumnIndex.downsampleColumnIndexList(columnIndex, 2);
        Assert.assertEquals(51, columnIndex.size());

        // 1st index shouldn't merge, so it should be the same as the original index
        Assert.assertEquals(0, columnIndex.get(0).offset);
        Assert.assertEquals(10, columnIndex.get(0).width);

        for(int i = 1; i < 50; i++) // 50 merged columns
        {
            // offset is offset by 10 because the first object wasnt merged
            Assert.assertEquals(10 + (20 * (i-1)), columnIndex.get(i).offset);
            Assert.assertEquals(20, columnIndex.get(i).width);
        }

        // 51/4 is not evenly divisible, the first 3 entries won't be merged
        ColumnIndex.downsampleColumnIndexList(columnIndex, 4);
        Assert.assertEquals(15, columnIndex.size());

        // 1st wasn't ever merged, so it should be the same as the original index
        Assert.assertEquals(0, columnIndex.get(0).offset);
        Assert.assertEquals(10, columnIndex.get(0).width);

        // Next 2 will match the first 2 from before
        for(int i = 1; i <= 2; i++)
        {
            // offset is offset by 10 because the first object wasnt merged
            Assert.assertEquals(10 + (20 * (i-1)), columnIndex.get(i).offset);
            Assert.assertEquals(20, columnIndex.get(i).width);
        }

        // The rest will have been merged with a factor of 4
        for(int i = 3; i < 15; i++)
        {
            // offsets that weren't merged here are 10, 20, 20, and then we'll have 4*20 in subsequent merges
            Assert.assertEquals(10 + 20 + 20 + (20 * 4 * (i-3)), columnIndex.get(i).offset);
            Assert.assertEquals(20 * 4, columnIndex.get(i).width);
        }

        // Sanity check
        Assert.assertEquals(15, columnIndex.size());

        // Merging by a number larger than the list is a no-op
        ColumnIndex.downsampleColumnIndexList(columnIndex, 16);
        Assert.assertEquals(15, columnIndex.size());

        // Merging by a number equal to the list creates a single item
        // (merges them ALL together)
        ColumnIndex.downsampleColumnIndexList(columnIndex, 15);
        Assert.assertEquals(1, columnIndex.size());
    }

}
