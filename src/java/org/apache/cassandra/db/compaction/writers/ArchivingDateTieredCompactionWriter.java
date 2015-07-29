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
package org.apache.cassandra.db.compaction.writers;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

import java.io.File;
import java.util.Set;

/**
  * CompactionAwareWriter that writes sstables to an conf.archive_data_file_directories
  *
  * This enables DTCS to move files that will no longer be compacted to bigger, slower, cheaper storage
  */
public class ArchivingDateTieredCompactionWriter extends CompactionAwareWriter
{
    private final long totalSize;
    private final Set<SSTableReader> allSSTables;
    private boolean useArchiveDataDirectory;

    public ArchivingDateTieredCompactionWriter(ColumnFamilyStore cfs, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, OperationType compactionType)
    {
        this(cfs, txn, nonExpiredSSTables, compactionType, false);
    }

    @SuppressWarnings("resource")
    public ArchivingDateTieredCompactionWriter(ColumnFamilyStore cfs, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, OperationType compactionType, boolean useArchiveDataDirectory)
    {
        super(cfs, txn, nonExpiredSSTables, false);
        this.allSSTables = txn.originals();
        this.useArchiveDataDirectory = useArchiveDataDirectory;
        // Even if we're not explicitly asking for archive, if any of the sstables are archived, the result will go to archival storage
        if (!useArchiveDataDirectory)
            for (SSTableReader s : nonExpiredSSTables)
                if (s.isArchivedDiskDirectory())
                    this.useArchiveDataDirectory = true;

        totalSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);

        File sstableDirectory = cfs.directories.getLocationForDisk(getWriteDirectory(totalSize, this.useArchiveDataDirectory));

        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(sstableDirectory)),
                                                    estimatedTotalKeys,
                                                    minRepairedAt,
                                                    cfs.metadata,
                                                    cfs.partitioner,
                                                    new MetadataCollector(allSSTables, cfs.metadata.comparator, 0));

        sstableWriter.switchWriter(writer);
    }

    @Override
    public boolean append(AbstractCompactedRow row)
    {
        RowIndexEntry rie = sstableWriter.append(row);
        return rie != null;
    }
}