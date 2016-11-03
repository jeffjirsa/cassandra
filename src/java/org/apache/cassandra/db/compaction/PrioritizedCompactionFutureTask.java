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

public class PrioritizedCompactionFutureTask<T> extends FutureTask<T> // implements IPrioritizedCompactionComparable
{
    final Priorities priorities;

    public PrioritizedCompactionFutureTask(Callable<T> callable)
    {
        super(callable);
        this.priorities = Priorities.DEFAULT;
    }

    public PrioritizedCompactionFutureTask(Callable<T> callable, Priorities priorities)
    {
        super(callable);
        this.priorities = priorities;
    }

    public PrioritizedCompactionFutureTask(Runnable runnable, T value)
    {
        super(runnable, value);
        this.priorities = Priorities.DEFAULT;
    }

    public PrioritizedCompactionFutureTask(Runnable runnable, T value, Priorities priorities)
    {
        super(runnable, value);
        this.priorities = priorities;
    }

    @Override
    public String toString()
    {
        return String.format("PrioritizedCompactionFutureTask(%s)", priorities);
    }

}
