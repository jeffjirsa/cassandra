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
package org.apache.cassandra.db.compaction;

public enum OperationType
{
    COMPACTION("Compaction") {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.compaction", "128")), 1);
        }
    },
    VALIDATION("Validation")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.validation", "256")),1);
        }
    },
    KEY_CACHE_SAVE("Key cache save")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.keycachesave", "256")),1);
        }
    },
    ROW_CACHE_SAVE("Row cache save")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.rowcachesave", "256")),1);
        }
    },
    COUNTER_CACHE_SAVE("Counter cache save")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.countercachesave", "256")),1);
        }
    },
    CLEANUP("Cleanup")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.cleanup", "64")),1);
        }
    },
    SCRUB("Scrub")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.scrub", "64")), 1);
        }
    },
    UPGRADE_SSTABLES("Upgrade sstables")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.upgrade", "64")),1);
        }
    },
    INDEX_BUILD("Secondary index build")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.indexbuild", "512")),1);
        }
    },
    /** Compaction for tombstone removal */
    TOMBSTONE_COMPACTION("Tombstone Compaction")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.tombstonecompaction", "96")),1);
        }
    },
    UNKNOWN("Unknown compaction type")
    {
        @Override
        public Integer priority()
        {
            return Integer.MAX_VALUE;
        }
    },
    ANTICOMPACTION("Anticompaction after repair")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.anticompaction", "1024")),1);
        }
    },
    VERIFY("Verify")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.verify", "1")),1);
        }
    },
    FLUSH("Flush")
    {
        @Override
        public Integer priority()
        {
            return Integer.MAX_VALUE;
        }
    },
    STREAM("Stream")
    {
        @Override
        public Integer priority()
        {
            return Integer.MAX_VALUE;
        }
    },
    WRITE("Write")    {
        @Override
        public Integer priority()
        {
            return Integer.MAX_VALUE;
        }
    },
    VIEW_BUILD("View build")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.viewbuild", "512")),1);
        }
    },
    INDEX_SUMMARY("Index summary redistribution")
    {
        @Override
        public Integer priority()
        {
            return Integer.MAX_VALUE;
        }
    },
    USER_DEFINED_COMPACTION("User defined compaction")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.user_defined_compaction", "192")),1);
        }
    },
    RELOCATE("Relocate sstables to correct disk")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.relocate", "192")), 1);
        }
    },
    GARBAGE_COLLECT("Remove deleted data")
    {
        @Override
        public Integer priority()
        {
            return Math.max(Integer.valueOf(System.getProperty("cassandra.compaction.priority.garbage_collection", "96")),1);
        }
    };

    public final String type;
    public final String fileName;

    OperationType(String type)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
    }

    public static OperationType fromFileName(String fileName)
    {
        for (OperationType opType : OperationType.values())
            if (opType.fileName.equals(fileName))
                return opType;

        throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
    }

    abstract public Integer priority();

    public String toString()
    {
        return type;
    }
}
