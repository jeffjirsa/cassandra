/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest extends SchemaLoader
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    private static final Random RANDOM = new Random();

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";
    public static final String KSNAME = "Keyspace1";
    public static final String CFNAME = "Standard1";

    public static Set<String> TEST_DATA_JB_ONLY;
    public static Set<String> TEST_DATA_NON_JB;
    public static File LEGACY_SSTABLE_ROOT;

    @BeforeClass
    public static void beforeClass()
    {
        Keyspace.setInitialized();
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();

        TEST_DATA_JB_ONLY = new HashSet<>();
        for (int i = 100; i < 1000; ++i)
            TEST_DATA_JB_ONLY.add(Integer.toString(i));

        TEST_DATA_NON_JB = new HashSet<>();
        for (int i = 0; i < 10; i++)
            TEST_DATA_NON_JB.add(Integer.toString(i));
    }

    @AfterClass
    public static void afterClass()
    {
        ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);
        for (SSTableReader reader : cfs.getSSTables())
        {
            reader.selfRef().release();
        }
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String ver)
    {
        File directory = new File(LEGACY_SSTABLE_ROOT + File.separator + ver + File.separator + KSNAME);
        return new Descriptor(ver, directory, KSNAME, CFNAME, 0, Descriptor.Type.FINAL);
    }

    /**
     * Generates a test SSTable for use in this classes' tests. Uncomment and run against an older build
     * and the output will be copied to a version subdirectory in 'LEGACY_SSTABLE_ROOT'
     *
     * @Test public void buildTestSSTable() throws IOException
     * {
     * // write the output in a version specific directory
     * Descriptor dest = getDescriptor(Descriptor.Version.current_version);
     * assert dest.directory.mkdirs() : "Could not create " + dest.directory + ". Might it already exist?";
     * <p>
     * SSTableReader ssTable = SSTableUtils.prepare().ks(KSNAME).cf(CFNAME).dest(dest).write(TEST_DATA);
     * assert ssTable.descriptor.generation == 0 :
     * "In order to create a generation 0 sstable, please run this test alone.";
     * System.out.println(">>> Wrote " + dest);
     * }
     */

    @Test
    public void testStreaming() throws Throwable
    {
        StorageService.instance.initServer();

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
            if (Descriptor.Version.validate(version.getName()) && new Descriptor.Version(version.getName()).isCompatible())
                testStreaming(version.getName());
    }

    private void testStreaming(String version) throws Exception
    {
        SSTableReader sstable = SSTableReader.open(getDescriptor(version));
        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(sstable.ref(),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("LegacyStreamingTest").transferFiles(FBUtilities.getBroadcastAddress(), details)
                                             .execute().get();

        loadLegacyTable(version);

        ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);
        assert cfs.getSSTables().size() == 1;

        sstable = cfs.getSSTables().iterator().next();
        CellNameType type = sstable.metadata.comparator;
        for (String keystring : ("jb".equals(version)) ? TEST_DATA_JB_ONLY : TEST_DATA_NON_JB)
        {
            ByteBuffer key = ByteBufferUtil.bytes(keystring);

            // get a sorted list of cell names that are expected to be in the key
            SortedSet<CellName> sortedCellNames = ("jb".equals(version))
                                                  ? FBUtilities.singleton(Util.cellname(key), type)
                                                  : getCellNames(keystring, type);

            SSTableNamesIterator iter = new SSTableNamesIterator(sstable, Util.dk(key), sortedCellNames);
            ColumnFamily cf = iter.getColumnFamily();

            // check not deleted (CASSANDRA-6527)
            assert cf.deletionInfo().equals(DeletionInfo.live());
            assert iter.next().name().toByteBuffer().equals(("jb".equals(version)) ? key : sortedCellNames.first().toByteBuffer());
        }
    }

    @Test
    public void testVersions() throws Throwable
    {
        boolean notSkipped = false;

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (Descriptor.Version.validate(version.getName()) && new Descriptor.Version(version.getName()).isCompatible())
            {
                notSkipped = true;
                if ("jb".equals(version.getName()))
                    testVersionJBOnly();
                else
                    testVersion(version.getName());
            }
        }

        assert notSkipped;
    }

    private static void loadLegacyTable(String legacyVersion) throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);

        for (File cfDir : cfs.getDataTracker().cfstore.directories.getCFDirectories())
        {
            // ensure we don't have any sstables from previous tests
            deleteAllFilesRecursively(cfDir);
            Collection<SSTableReader> previouslyLoadedSSTables = cfs.getSSTables();
            cfs.getDataTracker().markCompactedSSTablesReplaced(previouslyLoadedSSTables, Collections.<SSTableReader>emptySet(), OperationType.CLEANUP);
            Assert.assertEquals(0, cfs.getSSTables().size());

            for (File version : LEGACY_SSTABLE_ROOT.listFiles())
            {
                if (legacyVersion.equals(version.getName()))
                {
                    copySSTablesToTestData(version.getAbsoluteFile(), cfDir);
                }
            }
        }

        cfs.loadNewSSTables();
    }

    /**
     * "Legacy" version that doesn't do much validation but at least
     * keeps the same level of covereage for jb sstables we've always
     * had. For newer versions there is better test covereage.
     */
    public void testVersionJBOnly() throws Throwable
    {
        try
        {
            SSTableReader reader = SSTableReader.open(getDescriptor("jb"));
            CellNameType type = reader.metadata.comparator;
            for (String keystring : TEST_DATA_JB_ONLY)
            {
                ByteBuffer key = ByteBufferUtil.bytes(keystring);
                // confirm that the bloom filter does not reject any keys/names
                DecoratedKey dk = reader.partitioner.decorateKey(key);
                SSTableNamesIterator iter = new SSTableNamesIterator(reader, dk, FBUtilities.singleton(Util.cellname(key), type));
                assert iter.next().name().toByteBuffer().equals(key);
            }

            // TODO actually test some reads
        }
        catch (Throwable e)
        {
            logger.error("Failed to read version jb");
            throw e;
        }
    }

    public void testVersion(String version) throws Throwable
    {
        try
        {
            SSTableReader reader = SSTableReader.open(getDescriptor(version));
            CellNameType type = reader.metadata.comparator;
            for (String keystring : TEST_DATA_NON_JB)
            {
                ByteBuffer key = ByteBufferUtil.bytes(keystring);
                // confirm that the bloom filter does not reject any keys/names
                DecoratedKey dk = reader.partitioner.decorateKey(key);

                // get a sorted list of cell names that are expected to be in the key
                SortedSet<CellName> sortedCellNames = getCellNames(keystring, type);

                List<Composite> ret = new ArrayList<>();
                try (SSTableNamesIterator iter = new SSTableNamesIterator(reader, dk, sortedCellNames))
                {
                    while (iter.hasNext())
                    {
                        ret.add(iter.next().name());
                    }
                }

                Assert.assertEquals(sortedCellNames.size(), ret.size());
                int numIterated = 0;
                Iterator<CellName> sortedCellNamesIterator = sortedCellNames.iterator();
                while (sortedCellNamesIterator.hasNext())
                {
                    CellName nextExpectedCellName = sortedCellNamesIterator.next();
                    Assert.assertEquals(nextExpectedCellName, ret.get(numIterated++));
                }
            }

            loadLegacyTable(version);

            ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);

            SlicePredicate sp = new SlicePredicate();
            sp.setSlice_range(new SliceRange());
            sp.getSlice_range().setCount(6000);
            sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
            sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);

            List<Row> rows = cfs.getRangeSlice(Util.range("0", "9"), null, ThriftValidation.asIFilter(sp, cfs.metadata, null), 6000);
            Assert.assertEquals(9, rows.size());
            int currentRowIdx = 0;
            for (Row row : rows)
            {
                Collection cells = row.cf.getSortedColumns();
                Assert.assertEquals((currentRowIdx < 4) ? 10 : 5000, cells.size());
                currentRowIdx++;
            }
        }
        catch (Throwable e)
        {
            logger.error("Failed to read version: {}", version);
            throw e;
        }
    }

    private static void deleteAllFilesRecursively(File dirToDeleteFrom) throws IOException
    {
        Files.walkFileTree(dirToDeleteFrom.toPath(), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir,
                                                     final BasicFileAttributes attrs) throws IOException
            {

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file,
                                             final BasicFileAttributes attrs) throws IOException
            {
                Files.delete(file);

                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static SortedSet<CellName> getCellNames(String keystring, CellNameType type)
    {
        // the source legacy sstables have 10 columns for keys 0-4
        // and 5000 columns for keys 5-9. This ensures that we will
        // properly trigger and exercize both the indexed and non-indexed
        // code paths, as we only create indexes if there is more than
        // 64kb of data in a given row.
        int expectedNumColumns = (Integer.parseInt(keystring) < 5) ? 10 : 5000;

        // attempt to randomly select a subset of the number of columns.
        // The idea here is to ensure the SSTableNamesIterator both returns
        // and skips the correct columns
        SortedSet<CellName> sortedCellNames = new TreeSet<>(type);
        for (int i = 0; i < expectedNumColumns; i++)
        {
            // pick a random number from 0-10 and check if it's even
            // and use that to randomly decide if we should add a column or not
            if ((RANDOM.nextInt(10) & 1) == 0)
                sortedCellNames.add(Util.cellname("col" + i));
        }

        return sortedCellNames;
    }

    private static void copySSTablesToTestData(File sourceTableDir, File cfDir) throws IOException
    {
        logger.debug("Copying source legacy SSTables from test resource dir {} to cfDir {}", sourceTableDir, cfDir);

        Path sourcePath = Paths.get(sourceTableDir.toString());
        Path targetPath = Paths.get(cfDir.toString());

        Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir,
                                                     final BasicFileAttributes attrs) throws IOException
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file,
                                             final BasicFileAttributes attrs) throws IOException
            {
                String sourceFilename = file.getFileName().toString();
                if (!sourceFilename.startsWith("."))
                    Files.copy(file, Paths.get(targetPath.toString(), sourceFilename));

                return FileVisitResult.CONTINUE;
            }
        });
    }
}
