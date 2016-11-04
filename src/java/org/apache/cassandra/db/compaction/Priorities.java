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

/**
 * Priorities offers a dual level comparable for the purpose
 * of ordering tasks within a prioritized queue
 *
 * The intent is to allow coarse ordering by OperationType,
 * and then more granular ordering of tasks within a type
 * by timestamp.
 */
public class Priorities implements Comparable<Priorities>
{
    public static final Priorities DEFAULT = new Priorities(TaskPriority.MIN, 0);
    public final TaskPriority priority;
    public final long timestamp;

    public Priorities(TaskPriority priority)
    {
        this(priority, System.currentTimeMillis());
    }

    private Priorities(TaskPriority priority, long timestamp)
    {
        this.priority = priority;
        this.timestamp = timestamp;
    }

    public int compareTo(Priorities o)
    {
        // If the task types have the same priority value, favor the one with the lowest timestamp
        if (priority.priority() == o.priority.priority())
        {
            return timestamp < o.timestamp
                   ? -1
                   : timestamp > o.timestamp ? 1 : 0;
        }

        return o.priority.priority() - priority.priority();
    }

    public String toString()
    {
        return String.format("Priorities[type:%s, timestamp: %s]", priority, timestamp);
    }
}

