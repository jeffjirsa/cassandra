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

import org.apache.cassandra.utils.WrappedRunnable;

abstract public class PrioritizedCompactionWrappedRunnable extends WrappedRunnable implements IPrioritizedCompactionComparable
{

    protected final int compactionTypePriority;
    protected final long compactionSubTypePriority;
    protected final long timestamp;


    protected PrioritizedCompactionWrappedRunnable(int typePriority)
    {
        super();
        this.compactionTypePriority = typePriority;
        this.compactionSubTypePriority = 0;
        this.timestamp = System.currentTimeMillis();
    }

    protected PrioritizedCompactionWrappedRunnable(int typePriority, long subTypePriority)
    {
        super();
        this.compactionTypePriority = typePriority;
        this.compactionSubTypePriority = subTypePriority;
        this.timestamp = System.currentTimeMillis();
    }

    public int getTypePriority()
    {
        return compactionTypePriority;
    }

    public long getSubTypePriority()
    {
        return compactionSubTypePriority;
    }

    public long getTimestamp()
    {
        return timestamp;
    }


    public String toString()
    {
        return String.format("PrioritizedCompactionWrappedRunnable(TypePriority=%s,SubTypePriority=%s,Timestamp=%s",
                             compactionTypePriority,
                             compactionSubTypePriority,
                             timestamp);
    }

}
