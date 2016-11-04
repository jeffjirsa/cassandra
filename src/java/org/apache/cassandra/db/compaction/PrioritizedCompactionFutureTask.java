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
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ListenableFuture;

public class PrioritizedCompactionFutureTask<T> extends FutureTask<T> implements ListenableFuture<T>, Prioritized
{
    final Priorities priorities;
    final ExecutionList listeners = new ExecutionList();

    public PrioritizedCompactionFutureTask(PrioritizedCompactionCallable<T> callable)
    {
        super(callable);
        this.priorities = callable.getPriorities();
    }

    public PrioritizedCompactionFutureTask(Callable<T> callable, Priorities priorities)
    {
        super(callable);
        this.priorities = priorities;
    }

    public PrioritizedCompactionFutureTask(Callable<T> callable)
    {
        super(callable);
        this.priorities = Priorities.DEFAULT;
    }

    public PrioritizedCompactionFutureTask(PrioritizedCompactionRunnable runnable, T value)
    {
        super(runnable, value);
        this.priorities = runnable.getPriorities();
    }

    public PrioritizedCompactionFutureTask(PrioritizedCompactionWrappedRunnable runnable, T value)
    {
        super(runnable, value);
        this.priorities = runnable.getPriorities();
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

    public Priorities getPriorities()
    {
        return priorities;
    }


    // addListener(runnable, executor) and done() enable this to implement RunnableFuture
    // which is necessary for some compaction tasks (AntiCompaction for example). However,
    // we cannot simply wrap a compaction task via ListenableFutureTask::create as the
    // prioritized aspect of the task gets lost. The most natural solution would be to
    // extend ListenableFutureTask, but its constructors are package private, so we do
    // this instead.
    public void addListener(Runnable runnable, Executor executor)
    {
        listeners.add(runnable, executor);
    }

    @Override
    protected void done()
    {
        listeners.execute();
    }

}
