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

package org.apache.cassandra.io.sstable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;

import static org.apache.cassandra.Util.column;

/**
 * Tests iteration across all keys in a given sstable
 */
public class KeyIteratorTest extends SchemaLoader
{
    @After
    public void cleanupAfterTest() throws Exception
    {
        final Keyspace keyspace = Keyspace.open("Keyspace1");
        final ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("Standard1");
        cfStore.getDataTracker().unreferenceSSTables();
    }

    /**
     * Ensures that iterating thru the primary index for an sstable with only 1 key
     * iterates exactly once.
     */
    @Test
    public void testKeyIterationAcrossSSTableWithSingleSegment() throws Exception
    {
        final Keyspace keyspace = Keyspace.open("Keyspace1");
        final ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("Standard1");
        final DecoratedKey ROW = Util.dk("row2");

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        cf.addColumn(column("col2", "val2", 1L));
        cf.addColumn(column("col3", "val3", 1L));
        cf.addColumn(column("col4", "val4", 1L));
        cf.addColumn(column("col5", "val5", 1L));
        cf.addColumn(column("col6", "val6", 1L));
        Mutation rm = new Mutation("Keyspace1", ROW.getKey(), cf);
        rm.apply();
        cfStore.forceBlockingFlush();

        SSTableReader sstable = cfStore.getSSTables().iterator().next();
        int keysIterated = 0;
        try (KeyIterator keyIterator = new KeyIterator(sstable.descriptor))
        {
            while (keyIterator.hasNext())
            {
                DecoratedKey dk = keyIterator.next();
                keysIterated++;
            }
        }

        Assert.assertEquals(1, keysIterated);
    }

    /**
     * Tests iterating thru all keys in a sstable and ensures we iterate thru
     * the exact number of keys that we wrote in and flushed
     */
    @Test
    public void testKeyIterationAcrossSSTableWithMultipleSegments() throws Exception
    {
        final Keyspace keyspace = Keyspace.open("Keyspace1");
        final ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("Standard1");

        int numKeysToInsert = 25;

        for (int i = 0; i < numKeysToInsert; i++)
        {
            final DecoratedKey ROW = Util.dk("row" + i);

            ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
            cf.addColumn(column("col1", "val1", 1L));
            cf.addColumn(column("col2", "val2", 1L));
            cf.addColumn(column("col3", "val3", 1L));
            cf.addColumn(column("col4", "val4", 1L));
            cf.addColumn(column("col5", "val5", 1L));
            cf.addColumn(column("col6", "val6", 1L));
            Mutation rm = new Mutation("Keyspace1", ROW.getKey(), cf);
            rm.apply();
        }

        cfStore.forceBlockingFlush();

        SSTableReader sstable = cfStore.getSSTables().iterator().next();
        int keysIterated = 0;
        try (KeyIterator keyIterator = new KeyIterator(sstable.descriptor))
        {
            while (keyIterator.hasNext())
            {
                DecoratedKey dk = keyIterator.next();
                keysIterated++;
            }
        }

        Assert.assertEquals(numKeysToInsert, keysIterated);
    }
}
