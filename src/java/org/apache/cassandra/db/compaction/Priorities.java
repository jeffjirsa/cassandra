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
 * Priorities offers a three level comparable for the purpose
 * of ordering tasks within a prioritized queue
 *
 * The intent is to allow coarse ordering by OperationType,
 * and then more granular ordering of tasks within a type
 * by the subtype and timestamp.
 *
 * Individual callers are free to set subtype as needed, though
 * no effort is made to guard against starvation
 */
public class Priorities implements Comparable<Priorities>
{
    public static final Priorities DEFAULT = new Priorities(0, 0, 0);
    public final int type;
    public final long subtype;
    public final long timestamp;

    public Priorities(int type)
    {
        this(type, 0, System.currentTimeMillis());
    }

    public Priorities(int type, long subtype)
    {
        this(type, subtype, System.currentTimeMillis());
    }

    public Priorities(int type, long subtype, long timestamp)
    {
        this.type = type;
        this.subtype = subtype;
        this.timestamp = timestamp;
    }

    public int compareTo(Priorities o)
    {
        if (type > o.type)
            return -1;
        if (type < o.type)
            return 1;
        if (subtype > o.subtype)
            return -1;
        if (subtype < o.subtype)
            return 1;

        // If same op type, and same sub priority
        // Favor the task with the lowest timestamp
        return timestamp < o.timestamp
               ? -1
               : timestamp > o.timestamp ? 1 : 0;
    }
}
