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
    COMPACTION("Compaction", 128),
    VALIDATION("Validation", 256),
    KEY_CACHE_SAVE("Key cache save", 256),
    ROW_CACHE_SAVE("Row cache save", 256),
    COUNTER_CACHE_SAVE("Counter cache save", 256),
    CLEANUP("Cleanup", 64),
    SCRUB("Scrub", 64),
    UPGRADE_SSTABLES("Upgrade sstables", 64),
    INDEX_BUILD("Secondary index build", 512),
    /** Compaction for tombstone removal */
    TOMBSTONE_COMPACTION("Tombstone Compaction", 96),
    UNKNOWN("Unknown compaction type", Integer.MAX_VALUE)
    {
        @Override
        public int priority()
        {
            return this.defaultPriority;
        }
    },
    ANTICOMPACTION("Anticompaction after repair", 1024),
    VERIFY("Verify", 1),
    FLUSH("Flush", Integer.MAX_VALUE)
    {
        @Override
        public int priority()
        {
            return this.defaultPriority;
        }
    },
    STREAM("Stream", Integer.MAX_VALUE)
    {
        @Override
        public int priority()
        {
            return this.defaultPriority;
        }
    },
    WRITE("Write", Integer.MAX_VALUE)
    {
        @Override
        public int priority()
        {
            return this.defaultPriority;
        }
    },
    VIEW_BUILD("View build", 512),
    INDEX_SUMMARY("Index summary redistribution", Integer.MAX_VALUE),
    USER_DEFINED_COMPACTION("User defined compaction", 192),
    RELOCATE("Relocate sstables to correct disk", 192),
    GARBAGE_COLLECT("Remove deleted data", 96);

    private static final String priorityPropertyRoot = "cassandra.compaction.priority.";

    public final String type;
    public final String fileName;
    private final int defaultPriority;

    OperationType(String type, int defaultPriority)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
        this.defaultPriority = defaultPriority;
    }

    public static OperationType fromFileName(String fileName)
    {
        for (OperationType opType : OperationType.values())
            if (opType.fileName.equals(fileName))
                return opType;

        throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
    }

    public Integer priority()
    {
        return Math.max(Integer.getInteger(priorityPropertyRoot + name().toLowerCase(), defaultPriority), 1);
    }

    public String toString()
    {
        return type;
    }
}
