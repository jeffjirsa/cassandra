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

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DirectoriesTest
{
    private static File tempDataDir;
    private static File tempArchiveDir;
    private static final String KS = "ks";
    private static final String[] CFS = new String[] { "cf1", "ks" };

    private static final Set<CFMetaData> CFM = new HashSet<>(CFS.length);

    private static final CFMetaData PARENT_CFM = new CFMetaData(KS, "cf", ColumnFamilyType.Standard, null);
    private static final CFMetaData INDEX_CFM = new CFMetaData(KS, "cf.idx", ColumnFamilyType.Standard, null, PARENT_CFM.cfId);

    private static final Map<String, List<File>> files = new HashMap<>();

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        for (String cf : CFS)
        {
            CFM.add(new CFMetaData(KS, cf, ColumnFamilyType.Standard, null));
        }

        tempDataDir = File.createTempFile("cassandra", "unittest");
        tempDataDir.delete(); // hack to create a temp dir
        tempDataDir.mkdir();

        tempArchiveDir = File.createTempFile("cassandraarchive", "unittest");
        tempArchiveDir.delete();
        tempArchiveDir.mkdir();

        Directories.overrideDataDirectoriesForTest(tempDataDir.getPath(), tempArchiveDir.getPath());
        // Create two fake data dir for tests, one using CF directories, one that do not.
        createTestFiles();
    }

    @AfterClass
    public static void afterClass()
    {
        Directories.resetDataDirectoriesAfterTest();
        FileUtils.deleteRecursive(tempDataDir);
    }

    private static void createTestFiles() throws IOException
    {
        for (CFMetaData cfm : CFM)
        {
            List<File> fs = new ArrayList<>();
            files.put(cfm.cfName, fs);
            File dir = cfDir(cfm);
            dir.mkdirs();

            createFakeSSTable(dir, cfm.cfName, 1, false, fs);
            createFakeSSTable(dir, cfm.cfName, 2, true, fs);

            File backupDir = new File(dir, Directories.BACKUPS_SUBDIR);
            backupDir.mkdir();
            createFakeSSTable(backupDir, cfm.cfName, 1, false, fs);

            File snapshotDir = new File(dir, Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            snapshotDir.mkdirs();
            createFakeSSTable(snapshotDir, cfm.cfName, 1, false, fs);
        }
    }

    private static void createFakeSSTable(File dir, String cf, int gen, boolean temp, List<File> addTo) throws IOException
    {
        Descriptor desc = new Descriptor(dir, KS, cf, gen, temp ? Descriptor.Type.TEMP : Descriptor.Type.FINAL);
        for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER })
        {
            File f = new File(desc.filenameFor(c));
            f.createNewFile();
            addTo.add(f);
        }
    }

    private static File cfDir(CFMetaData metadata)
    {
        String cfId = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(metadata.cfId));
        int idx = metadata.cfName.indexOf(Directories.SECONDARY_INDEX_NAME_SEPARATOR);
        if (idx >= 0)
        {
            // secondary index
            return new File(tempDataDir,
                            metadata.ksName + File.separator +
                            metadata.cfName.substring(0, idx) + '-' + cfId + File.separator +
                            metadata.cfName.substring(idx));
        }
        else
        {
            return new File(tempDataDir, metadata.ksName + File.separator + metadata.cfName + '-' + cfId);
        }
    }

    @Test
    public void testStandardDirs()
    {
        for (CFMetaData cfm : CFM)
        {
            Directories directories = new Directories(cfm);
            assertEquals(cfDir(cfm), directories.getDirectoryForNewSSTables());

            Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.cfName, 1, Descriptor.Type.FINAL);
            File snapshotDir = new File(cfDir(cfm),  File.separator + Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            assertEquals(snapshotDir, Directories.getSnapshotDirectory(desc, "42"));

            File backupsDir = new File(cfDir(cfm),  File.separator + Directories.BACKUPS_SUBDIR);
            assertEquals(backupsDir, Directories.getBackupsDirectory(desc));
        }
    }

    @Test
    public void testSecondaryIndexDirectories()
    {
        Directories parentDirectories = new Directories(PARENT_CFM);
        Directories indexDirectories = new Directories(INDEX_CFM);
        // secondary index has its own directory
        boolean checked_index_directory = false;
        for (File dir : indexDirectories.getCFDirectories())
        {
            // Ignore the archive directory
            if(!dir.toString().contains("archive"))
            {
                assertEquals(cfDir(INDEX_CFM), dir);
                checked_index_directory = true;
            }
        }
        assertEquals(checked_index_directory, true);

        Descriptor parentDesc = new Descriptor(parentDirectories.getDirectoryForNewSSTables(), KS, PARENT_CFM.cfName, 0, Descriptor.Type.FINAL);
        Descriptor indexDesc = new Descriptor(indexDirectories.getDirectoryForNewSSTables(), KS, INDEX_CFM.cfName, 0, Descriptor.Type.FINAL);

        // snapshot dir should be created under its parent's
        File parentSnapshotDirectory = Directories.getSnapshotDirectory(parentDesc, "test");
        File indexSnapshotDirectory = Directories.getSnapshotDirectory(indexDesc, "test");
        assertEquals(parentSnapshotDirectory, indexSnapshotDirectory.getParentFile());

        // check if snapshot directory exists
        parentSnapshotDirectory.mkdirs();
        assertTrue(parentDirectories.snapshotExists("test"));
        assertTrue(indexDirectories.snapshotExists("test"));

        // check their creation time
        assertEquals(parentDirectories.snapshotCreationTime("test"),
                     indexDirectories.snapshotCreationTime("test"));

        // check true snapshot size
        Descriptor parentSnapshot = new Descriptor(parentSnapshotDirectory, KS, PARENT_CFM.cfName, 0, Descriptor.Type.FINAL);
        createFile(parentSnapshot.filenameFor(Component.DATA), 30);
        Descriptor indexSnapshot = new Descriptor(indexSnapshotDirectory, KS, INDEX_CFM.cfName, 0, Descriptor.Type.FINAL);
        createFile(indexSnapshot.filenameFor(Component.DATA), 40);

        assertEquals(30L, parentDirectories.trueSnapshotsSize());
        assertEquals(40L, indexDirectories.trueSnapshotsSize());

        // check snapshot details
        Map<String, Pair<Long, Long>> parentSnapshotDetail = parentDirectories.getSnapshotDetails();
        assertTrue(parentSnapshotDetail.containsKey("test"));
        assertEquals(30L, parentSnapshotDetail.get("test").right.longValue());

        Map<String, Pair<Long, Long>> indexSnapshotDetail = indexDirectories.getSnapshotDetails();
        assertTrue(indexSnapshotDetail.containsKey("test"));
        assertEquals(40L, indexSnapshotDetail.get("test").right.longValue());

        // check backup directory
        File parentBackupDirectory = Directories.getBackupsDirectory(parentDesc);
        File indexBackupDirectory = Directories.getBackupsDirectory(indexDesc);
        assertEquals(parentBackupDirectory, indexBackupDirectory.getParentFile());
    }

    private File createFile(String fileName, int size)
    {
        File newFile = new File(fileName);
        try (FileOutputStream writer = new FileOutputStream(newFile))
        {
            writer.write(new byte[size]);
            writer.flush();
        }
        catch (IOException ignore) {}
        return newFile;
    }

    @Test
    public void testSSTableLister()
    {
        for (CFMetaData cfm : CFM)
        {
            Directories directories = new Directories(cfm);
            Directories.SSTableLister lister;
            Set<File> listed;

            // List all but no snapshot, backup
            lister = directories.sstableLister();
            listed = new HashSet<>(lister.listFiles());
            for (File f : files.get(cfm.cfName))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }

            // List all but including backup (but no snapshot)
            lister = directories.sstableLister().includeBackups(true);
            listed = new HashSet<>(lister.listFiles());
            for (File f : files.get(cfm.cfName))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }

            // Skip temporary and compacted
            lister = directories.sstableLister().skipTemporary(true);
            listed = new HashSet<>(lister.listFiles());
            for (File f : files.get(cfm.cfName))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else if (f.getName().contains("tmp-"))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }
        }
    }


    @Test
    public void testDiskFailurePolicy_best_effort()
    {
        DiskFailurePolicy origPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        
        try 
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.best_effort);
            // Fake a Directory creation failure
            if (Directories.dataDirectories.length > 0)
            {
                String[] path = new String[] {KS, "bad"};
                File dir = new File(Directories.dataDirectories[0].location, StringUtils.join(path, File.separator));
                FileUtils.handleFSError(new FSWriteError(new IOException("Unable to create directory " + dir), dir));
            }

            boolean checked_unwritable = false;
            for (DataDirectory dd : Directories.dataDirectories)
            {
                File file = new File(dd.location, new File(KS, "bad").getPath());
                if(!file.toString().contains("cassandraarchive"))
                {
                    assertTrue(BlacklistedDirectories.isUnwritable(file));
                    checked_unwritable = true;
                }
            }
            assertEquals(checked_unwritable, true);
        } 
        finally 
        {
            DatabaseDescriptor.setDiskFailurePolicy(origPolicy);
        }
    }

    @Test
    public void testMTSnapshots() throws Exception
    {
        for (final CFMetaData cfm : CFM)
        {
            final Directories directories = new Directories(cfm);
            assertEquals(cfDir(cfm), directories.getDirectoryForNewSSTables());
            final String n = Long.toString(System.nanoTime());
            Callable<File> directoryGetter = new Callable<File>() {
                public File call() throws Exception {
                    Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.cfName, 1, Descriptor.Type.FINAL);
                    return Directories.getSnapshotDirectory(desc, n);
                }
            };
            List<Future<File>> invoked = Executors.newFixedThreadPool(2).invokeAll(Arrays.asList(directoryGetter, directoryGetter));
            for(Future<File> fut:invoked) {
                assertTrue(fut.get().exists());
            }
        }
    }

    @Test
    public void testDiskFreeSpace()
    {
        DataDirectory[] dataDirectories = new DataDirectory[]
                                          {
                                          new DataDirectory(new File("/nearlyFullDir1"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 11L;
                                              }
                                          },
                                          new DataDirectory(new File("/nearlyFullDir2"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 10L;
                                              }
                                          },
                                          new DataDirectory(new File("/uniformDir1"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 1000L;
                                              }
                                          },
                                          new DataDirectory(new File("/uniformDir2"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 999L;
                                              }
                                          },
                                          new DataDirectory(new File("/veryFullDir"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 4L;
                                              }
                                          }
                                          };

        // directories should be sorted
        // 1. by their free space ratio
        // before weighted random is applied
        List<Directories.DataDirectoryCandidate> candidates = getWriteableDirectories(dataDirectories, 0L);
        assertSame(dataDirectories[2], candidates.get(0).dataDirectory); // available: 1000
        assertSame(dataDirectories[3], candidates.get(1).dataDirectory); // available: 999
        assertSame(dataDirectories[0], candidates.get(2).dataDirectory); // available: 11
        assertSame(dataDirectories[1], candidates.get(3).dataDirectory); // available: 10

        // check for writeSize == 5
        Map<DataDirectory, DataDirectory> testMap = new IdentityHashMap<>();
        for (int i=0; ; i++)
        {
            candidates = getWriteableDirectories(dataDirectories, 5L);
            assertEquals(4, candidates.size());

            DataDirectory dir = Directories.pickWriteableDirectory(candidates);
            testMap.put(dir, dir);

            assertFalse(testMap.size() > 4);
            if (testMap.size() == 4)
            {
                // at least (rule of thumb) 100 iterations to see whether there are more (wrong) directories returned
                if (i >= 100)
                    break;
            }

            // random weighted writeable directory algorithm fails to return all possible directories after
            // many tries
            if (i >= 10000000)
                fail();
        }

        // check for writeSize == 11
        testMap.clear();
        for (int i=0; ; i++)
        {
            candidates = getWriteableDirectories(dataDirectories, 11L);
            assertEquals(3, candidates.size());
            for (Directories.DataDirectoryCandidate candidate : candidates)
                assertTrue(candidate.dataDirectory.getAvailableSpace() >= 11L);

            DataDirectory dir = Directories.pickWriteableDirectory(candidates);
            testMap.put(dir, dir);

            assertFalse(testMap.size() > 3);
            if (testMap.size() == 3)
            {
                // at least (rule of thumb) 100 iterations
                if (i >= 100)
                    break;
            }

            // random weighted writeable directory algorithm fails to return all possible directories after
            // many tries
            if (i >= 10000000)
                fail();
        }
    }

    private List<Directories.DataDirectoryCandidate> getWriteableDirectories(DataDirectory[] dataDirectories, long writeSize)
    {
        // copied from Directories.getWriteableLocation(long)
        List<Directories.DataDirectoryCandidate> candidates = new ArrayList<>();

        long totalAvailable = 0L;

        for (DataDirectory dataDir : dataDirectories)
            {
                Directories.DataDirectoryCandidate candidate = new Directories.DataDirectoryCandidate(dataDir);
                // exclude directory if its total writeSize does not fit to data directory
                if (candidate.availableSpace < writeSize)
                    continue;
                candidates.add(candidate);
                totalAvailable += candidate.availableSpace;
            }

        Directories.sortWriteableCandidates(candidates, totalAvailable);

        return candidates;
    }
}
