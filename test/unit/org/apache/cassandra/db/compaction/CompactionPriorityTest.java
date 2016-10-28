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

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Test;

import org.apache.cassandra.db.compaction.CompactionManager.CompactionPriorityComparator;

import static org.junit.Assert.assertEquals;

public class CompactionPriorityTest
{
    @Test
    public void testPrioritization()
    {
        PriorityBlockingQueue<Runnable> priorityBlockingQueue = new PriorityBlockingQueue<>(10, new CompactionPriorityComparator());

        // Queue up a bunch of stub runnables of various types priorities
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.COMPACTION.priority())
        {
            public int hashCode() { return 1; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionRunnable(OperationType.COMPACTION.priority())
        {
            public int hashCode() { return 2; }

            public void run() { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.SCRUB.priority())
        {
            public int hashCode() { return 3; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.VERIFY.priority())
        {
            public int hashCode() { return 4; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.ANTICOMPACTION.priority())
        {
            public int hashCode() { return 5; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.INDEX_BUILD.priority())
        {
            public int hashCode() { return 6; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.KEY_CACHE_SAVE.priority())
        {
            public int hashCode() { return 7; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.CLEANUP.priority())
        {
            public int hashCode() { return 8; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.COMPACTION.priority(), 100L)
        {
            public int hashCode() { return 9; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionWrappedRunnable(OperationType.COMPACTION.priority(), 500L)
        {
            public int hashCode() { return 10; }

            protected void runMayThrow() throws Exception { }
        });
        priorityBlockingQueue.add(new PrioritizedCompactionRunnable(OperationType.UPGRADE_SSTABLES.priority(), 500000L)
        {
            public int hashCode() { return 11; }

            public void run() { }
        });


        Runnable r;
        r = priorityBlockingQueue.poll();
        assertEquals(5, r.hashCode()); // Anticompaction
        r = priorityBlockingQueue.poll();
        assertEquals(6, r.hashCode()); // Index build
        r = priorityBlockingQueue.poll();
        assertEquals(7, r.hashCode()); // Key cache save

        // For compaction, use the subtype priority, largest first
        r = priorityBlockingQueue.poll();
        assertEquals(10, r.hashCode()); // Compaction (10 is first based on size)
        r = priorityBlockingQueue.poll();
        assertEquals(9, r.hashCode()); // Compaction (9 has subtype set, but is lower than 10)

        // For non-subtype compaction, we use timestamp resolution
        r = priorityBlockingQueue.poll();
        assertEquals(1, r.hashCode()); // Compaction (1 is first due to timestamps)
        r = priorityBlockingQueue.poll();
        assertEquals(2, r.hashCode()); // Compaction

        // Scrub/Cleanup below Compaction
        r = priorityBlockingQueue.poll();
        assertEquals(11, r.hashCode());// Upgrade SSTables ahead of scrub/cleanup because of sub priority
        r = priorityBlockingQueue.poll();
        assertEquals(3, r.hashCode()); // Scrub
        r = priorityBlockingQueue.poll();
        assertEquals(8, r.hashCode()); // Cleanup (behind scrub due to timestamps)
        r = priorityBlockingQueue.poll();
        assertEquals(4, r.hashCode()); // Verify
    }
}
