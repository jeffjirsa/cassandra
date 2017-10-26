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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.metrics.StorageMetrics;
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

        // 101/2 is not evenly divisible, the last entry won't be merged
        // but it will be nulled out so grab a copy of its offset & width
        // now to compare against after downsampling
        long expectedOffset = columnIndex.get(100).offset;
        long expectedWidth = columnIndex.get(100).width;

        columnIndex = ColumnIndex.downsampleColumnIndexList(columnIndex, 2);
        Assert.assertEquals(51, columnIndex.size());

        // the first 50 elements should just be pairwise merges from the input
        for(int i = 0; i < 50; i++)
        {
            Assert.assertEquals(20 * i, columnIndex.get(i).offset);
            Assert.assertEquals(20, columnIndex.get(i).width);
        }

        // last index shouldn't merge, so it should be the same as the original index
        Assert.assertEquals(expectedOffset, columnIndex.get(columnIndex.size() - 1).offset);
        Assert.assertEquals(expectedWidth, columnIndex.get(columnIndex.size() - 1).width);

        // 51/4 is not evenly divisible, so the final entry in the downsampled list
        // should be the merge of the last 3 elements in the input.
        // Capture the initial offset & combined width of those 3 to check against
        expectedOffset = columnIndex.get(columnIndex.size() - 3).offset;
        expectedWidth = 0;
        for(int i = columnIndex.size() - 3; i < columnIndex.size(); i++)
        {
            expectedWidth += columnIndex.get(i).width;
        }

        columnIndex = ColumnIndex.downsampleColumnIndexList(columnIndex, 4);
        Assert.assertEquals(13, columnIndex.size());

        // The first 12 elements are simply merges of 4 element blocks from the input
        for(int i = 1; i < 12; i++)
        {
            // the input list had elements with offsets 20 apart, so these should be spaced out 4 * 20
            Assert.assertEquals(((20 * 4) * i), columnIndex.get(i).offset);
            Assert.assertEquals(20 * 4, columnIndex.get(i).width);
        }

        // The modulo final 3 elements from the input should be merged into the
        // final 13th element of the downsampled list
        Assert.assertEquals(expectedOffset, columnIndex.get(columnIndex.size() - 1).offset);
        Assert.assertEquals(expectedWidth, columnIndex.get(columnIndex.size() - 1).width);

        // Sanity check
        Assert.assertEquals(13, columnIndex.size());

        // Merging by a number equal to the list creates a single item
        // (merges them ALL together)
        columnIndex = ColumnIndex.downsampleColumnIndexList(columnIndex, 13);
        Assert.assertEquals(1, columnIndex.size());
    }

    @Test
    public void testWidePartitionWithDownsampling() throws Throwable
    {
        testWidePartitionParameterized(61, true);
    }

    @Test
    public void testWidePartitionWithoutDownsampling() throws Throwable
    {
        testWidePartitionParameterized(Integer.MAX_VALUE / 1024, false);
    }

    @Test
    public void testWidePartitionWithMassiveDownsampling() throws Throwable
    {
        testWidePartitionParameterized(2, true);
    }

    @Test
    public void testWidePartitionWithInvalidDownsampling() throws Throwable
    {
        try
        {
            testWidePartitionParameterized(-1, false);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // Expected
        }
    }

    @Test
    public void testWidePartitionWithInvalidDownsamplingHigh() throws Throwable
    {
        testWidePartitionParameterized(Integer.MAX_VALUE, false);
    }

    @Test
    public void testWidePartitionWithDownsamplingBySize() throws Throwable
    {
        testWidePartitionParameterizedBySize(64 * 1024, true);
    }

    @Test
    public void testWidePartitionWithoutDownsamplingBySize() throws Throwable
    {
        testWidePartitionParameterizedBySize(Integer.MAX_VALUE, false);
    }

    @Test
    public void testWidePartitionWithMassiveDownsamplingBySize() throws Throwable
    {
        testWidePartitionParameterizedBySize(1024, true);
    }


    @Test
    public void testWidePartitionWithInvalidDownsamplingBySize() throws Throwable
    {
        try
        {
            testWidePartitionParameterizedBySize(-1, false);
            Assert.fail();
        }
        catch(IllegalArgumentException e)
        {
            // Expected
        }
    }

    @Test
    public void testWidePartitionWithTombstonesDownsampling() throws Throwable
    {
        testWidePartitionWithTombstones(61, true);
    }

    @Test
    public void testWidePartitionWithTombstonesWithoutDownsampling() throws Throwable
    {
        testWidePartitionWithTombstones(Integer.MAX_VALUE / 1024, false);
    }

    @Test
    public void testWidePartitionWithTombstonesWithMassiveDownsampling() throws Throwable
    {
        testWidePartitionWithTombstones(2, true);
    }


    public void testWidePartitionParameterized(int maxCount, boolean shouldDownsample) throws Throwable
    {
        int protocolVersion = 4;
        long startDownsamples = StorageMetrics.columnIndexDownsamples.getCount();

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "intval int, " +
                    "dummystring text, " +
                    "PRIMARY KEY (k, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        StringBuilder stringBuilder = new StringBuilder(4096);
        for (int i = 0; i < 4096; i++)
            stringBuilder.append("A");

        String dummyString = stringBuilder.toString();

        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, intval, dummystring) VALUES (?, ?, ?, ?)", 0, i, i, dummyString);

        // We inserted 1024 rows, each with 4k+4 bytes, +/- some overhead, so we should have at least 4M in the partition
        // Which with the default 64k chunk gives us more than 64 columnindex objects

        // Now we'll force a downsample, and we'll pick something that's unlikely to be evenly divisible
        int restore = DatabaseDescriptor.getColumnIndexMaxCount();
        DatabaseDescriptor.setColumnIndexMaxCount(maxCount);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        getCurrentColumnFamilyStore().forceMajorCompaction();

        // Again make sure we have all the rows in a normal scan
        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));

        // Check each row
        for(int i = 0; i < 1024; i++)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c = ? ", 0, i), row(i));

        // Slices (descending looking backward)
        for(int i = 1023; i >= 1; i--)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? ORDER BY c DESC LIMIT 2 ", 0, i), row(i), row(i - 1));

        // Slices (ascending looking forward)
        for(int i = 0; i + 1 < 1024; i++)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? ORDER BY c ASC LIMIT 2 ", 0, i), row(i), row(i + 1));

        // Slices (descending looking forward)
        // Start at 1022 because we'll look forward to 1023
        for(int i = 1022; i >= 0; i--)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? AND c < ? ORDER BY c DESC LIMIT 2 ", 0, i, i + 2), row(i + 1), row(i));

        // Slices (ascending looking backward)
        // Start at 1 because we'll look back to 0
        for(int i = 1; i + 1 < 1024; i++)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? AND c > ? ORDER BY c ASC LIMIT 2 ", 0, i, i - 2), row(i-1), row(i ));


        DatabaseDescriptor.setColumnIndexMaxCount(restore);

        long endDownsamples = StorageMetrics.columnIndexDownsamples.getCount();
        if (shouldDownsample)
            Assert.assertTrue(startDownsamples < endDownsamples);
        else
            Assert.assertEquals(startDownsamples, endDownsamples);

    }

    public void testWidePartitionParameterizedBySize(int maxSize, boolean shouldDownsample) throws Throwable
    {
        int protocolVersion = 4;
        long startDownsamples = StorageMetrics.columnIndexDownsamples.getCount();

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "c2 text, " +
                    "intval int, " +
                    "dummystring text, " +
                    "PRIMARY KEY (k, c, c2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        StringBuilder stringBuilder = new StringBuilder(4096);
        for (int i = 0; i < 4096; i++)
            stringBuilder.append("A");

        String dummyString = stringBuilder.toString();

        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, c2, intval, dummystring) VALUES (?, ?, ?, ?, ?)", 0, i, dummyString, i, dummyString);

        // We inserted 1024 rows, each with 4k+4k+4 bytes, +/- some overhead, so we should have at least 8M in the partition
        // Which with the default 64k chunk gives us more than 128 columnindex objects
        // But more importantly, the size of those 128 objects should be larger than 128kb

        // Now we'll force a downsample, and we'll pick something that's unlikely to be evenly divisible
        int restore = DatabaseDescriptor.getColumnIndexMaxSizeInBytes();

        DatabaseDescriptor.setColumnIndexMaxSizeInBytes(maxSize);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        getCurrentColumnFamilyStore().forceMajorCompaction();

        // Again make sure we have all the rows in a normal scan
        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));

        // Check each row
        for(int i = 0; i < 1024; i++)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c = ? ", 0, i), row(i));

        // Slices (descending looking backward)
        for(int i = 1023; i >= 1; i--)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? ORDER BY c DESC LIMIT 2 ", 0, i), row(i), row(i - 1));

        // Slices (ascending looking forward)
        for(int i = 0; i + 1 < 1024; i++)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? ORDER BY c ASC LIMIT 2 ", 0, i), row(i), row(i + 1));

        // Slices (descending looking forward)
        // Start at 1022 because we'll look forward to 1023
        for(int i = 1022; i >= 0; i--)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? AND c < ? ORDER BY c DESC LIMIT 2 ", 0, i, i + 2), row(i + 1), row(i));

        // Slices (ascending looking backward)
        // Start at 1 because we'll look back to 0
        for(int i = 1; i + 1 < 1024; i++)
            assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? AND c > ? ORDER BY c ASC LIMIT 2 ", 0, i, i - 2), row(i-1), row(i ));

        DatabaseDescriptor.setColumnIndexMaxSizeInBytes(restore);

        long endDownsamples = StorageMetrics.columnIndexDownsamples.getCount();
        if (shouldDownsample)
            Assert.assertTrue(startDownsamples < endDownsamples);
        else
            Assert.assertEquals(startDownsamples, endDownsamples);

    }

    public void testWidePartitionWithTombstones(int maxCount, boolean shouldDownsample) throws Throwable
    {
        int protocolVersion = 4;
        long startDownsamples = StorageMetrics.columnIndexDownsamples.getCount();

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "c2 text, " +
                    "intval int, " +
                    "dummystring text, " +
                    "PRIMARY KEY (k, c, c2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        StringBuilder stringBuilder = new StringBuilder(4096);
        for (int i = 0; i < 4096; i++)
            stringBuilder.append("A");

        String dummyString = stringBuilder.toString();

        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, c2, intval, dummystring) VALUES (?, ?, ?, ?, ?)", 0, i, dummyString, i, dummyString);

        getCurrentColumnFamilyStore().disableAutoCompaction();
        getCurrentColumnFamilyStore().forceBlockingFlush();

        for(int i = 0; i < 1024; i++)
            if ( i % 2 == 0)
                execute("DELETE FROM %s WHERE k=? and c >= ? and c < ?", 0, i, i+1);

        // We inserted 1024 rows, each with 4k+4 bytes, +/- some overhead, so we should have at least 4M in the partition
        // Which with the default 64k chunk gives us more than 64 columnindex objects

        // Now we'll force a downsample, and we'll pick something that's unlikely to be evenly divisible
        int restore = DatabaseDescriptor.getColumnIndexMaxCount();
        DatabaseDescriptor.setColumnIndexMaxCount(maxCount);
        getCurrentColumnFamilyStore().forceBlockingFlush();

        // Again make sure we have all the rows in a normal scan
        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(512L));

        // Check each row
        for(int i = 0; i < 1024; i++)
        {
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c = ? ", 0, i), row(i));
            else
                assertEmpty(execute("SELECT intval FROM %s WHERE k = ? AND c = ? ", 0, i));

        }

        // Slices (descending looking backward)
        for(int i = 1023; i >= 3; i--)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? ORDER BY c DESC LIMIT 2 ", 0, i), row(i), row(i - 2));

        // Slices (ascending looking forward)
        for(int i = 2; i + 1 < 1022; i++)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? ORDER BY c ASC LIMIT 2 ", 0, i), row(i), row(i + 2));

        // Slices (descending looking forward)
        // Start at 1021 because we'll look forward to 1023
        for(int i = 1021; i >= 2; i--)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? AND c < ? ORDER BY c DESC LIMIT 2 ", 0, i, i + 3), row(i + 2), row(i));

        // Slices (ascending looking backward)
        // Start at 2 because we'll look back to 0
        for(int i = 2; i + 1 < 1024; i++)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? AND c > ? ORDER BY c ASC LIMIT 2 ", 0, i, i - 3), row(i-2), row(i ));

        // Now major compaction and do it again
        getCurrentColumnFamilyStore().forceMajorCompaction();

        // Again make sure we have all the rows in a normal scan
        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(512L));

        // Check each row
        for(int i = 0; i < 1024; i++)
        {
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c = ? ", 0, i), row(i));
            else
                assertEmpty(execute("SELECT intval FROM %s WHERE k = ? AND c = ? ", 0, i));

        }

        // Slices (descending looking backward)
        for(int i = 1023; i >= 3; i--)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? ORDER BY c DESC LIMIT 2 ", 0, i), row(i), row(i - 2));

        // Slices (ascending looking forward)
        for(int i = 2; i + 1 < 1022; i++)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? ORDER BY c ASC LIMIT 2 ", 0, i), row(i), row(i + 2));

        // Slices (descending looking forward)
        // Start at 1021 because we'll look forward to 1023
        for(int i = 1021; i >= 2; i--)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c >= ? AND c < ? ORDER BY c DESC LIMIT 2 ", 0, i, i + 3), row(i + 2), row(i));

        // Slices (ascending looking backward)
        // Start at 2 because we'll look back to 0
        for(int i = 2; i + 1 < 1024; i++)
            if (i % 2 == 1)
                assertRows(execute("SELECT intval FROM %s WHERE k = ? AND c <= ? AND c > ? ORDER BY c ASC LIMIT 2 ", 0, i, i - 3), row(i-2), row(i ));



        DatabaseDescriptor.setColumnIndexMaxCount(restore);

        long endDownsamples = StorageMetrics.columnIndexDownsamples.getCount();
        if (shouldDownsample)
            Assert.assertTrue(startDownsamples < endDownsamples);
        else
            Assert.assertEquals(startDownsamples, endDownsamples);

    }

}
