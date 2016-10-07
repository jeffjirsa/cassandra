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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PrioritizedCompactionFutureTask<T> extends FutureTask<T> implements IPrioritizedCompactionComparable
{
    protected final AtomicInteger compactionTypePriority;
    protected final AtomicLong compactionSubTypePriority;
    protected final AtomicLong taskTimestamp;

    public PrioritizedCompactionFutureTask(Callable<T> callable, Integer typePriority, Long subTypePriority)
    {
        super(callable);
        this.compactionTypePriority = new AtomicInteger(typePriority);
        this.compactionSubTypePriority = new AtomicLong(subTypePriority);
        this.taskTimestamp = new AtomicLong(System.currentTimeMillis());
    }

    public PrioritizedCompactionFutureTask(Runnable runnable, T value, Integer typePriority, Long subTypePriority)
    {
        super(runnable, value);
        this.compactionTypePriority = new AtomicInteger(typePriority);
        this.compactionSubTypePriority = new AtomicLong(subTypePriority);
        this.taskTimestamp = new AtomicLong(System.currentTimeMillis());
    }


    public Integer getTypePriority()
    {
        return this.compactionTypePriority.get();
    }

    public Long getSubTypePriority()
    {
        return this.compactionSubTypePriority.get();
    }

    public Long getTimestamp()
    {
        return taskTimestamp.get();
    }

    @Override
    public String toString()
    {
        return String.format("PrioritizedCompactionFutureTask(TypePriority=%s,SubTypePriority=%s,Timestamp=%s",
                             compactionTypePriority.get(),
                             compactionSubTypePriority.get(),
                             taskTimestamp.get());
    }

}
