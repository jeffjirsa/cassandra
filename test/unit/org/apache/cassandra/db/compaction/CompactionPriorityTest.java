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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompactionPriorityTest
{
    @Test
    public void testPrioritization() throws InterruptedException
    {
        PriorityBlockingQueue<Runnable> blockingQueue =
            new PriorityBlockingQueue<>(10, new CompactionManager.CompactionPriorityComparator());

        // Queue up a bunch of stub runnables of various types priorities, sleeping for 1ms between
        // each to ensure their priorities have distinct timestamps
        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.COMPACTION)
        {
            public int hashCode() { return 1; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionRunnable(TaskPriority.COMPACTION)
        {
            public int hashCode() { return 2; }

            public void run() { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.SCRUB)
        {
            public int hashCode() { return 3; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.VERIFY)
        {
            public int hashCode() { return 4; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.ANTICOMPACTION)
        {
            public int hashCode() { return 5; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.INDEX_BUILD)
        {
            public int hashCode() { return 6; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.KEY_CACHE_SAVE)
        {
            public int hashCode() { return 7; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.CLEANUP)
        {
            public int hashCode() { return 8; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.COMPACTION)
        {
            public int hashCode() { return 10; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionWrappedRunnable(TaskPriority.COMPACTION)
        {
            public int hashCode() { return 9; }

            protected void runMayThrow() throws Exception { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        blockingQueue.add(new PrioritizedCompactionRunnable(TaskPriority.UPGRADE_SSTABLES)
        {
            public int hashCode() { return 11; }

            public void run() { }
        });
        TimeUnit.MILLISECONDS.sleep(1);

        Runnable r = blockingQueue.poll();
        assertEquals(5, r.hashCode()); // Anticompaction
        r = blockingQueue.poll();
        assertEquals(6, r.hashCode()); // Index build
        r = blockingQueue.poll();
        assertEquals(7, r.hashCode()); // Key cache save

        // Regular compaction
        r = blockingQueue.poll();
        assertEquals(1, r.hashCode()); // Compaction
        r = blockingQueue.poll();
        assertEquals(2, r.hashCode()); // Compaction
        r = blockingQueue.poll();
        assertEquals(10, r.hashCode()); // Compaction (10 is before 9 based on timestamp)
        r = blockingQueue.poll();
        assertEquals(9, r.hashCode()); // Compaction (9 has same type as 10, but was created after)

        // Scrub/Cleanup below Compaction and ordered by timestamp
        blockingQueue.forEach(System.out::println);
        r = blockingQueue.poll();
        assertEquals(3, r.hashCode()); // Scrub
        r = blockingQueue.poll();
        assertEquals(8, r.hashCode()); // Cleanup
        r = blockingQueue.poll();
        assertEquals(11, r.hashCode());// Upgrade SSTables

        // Verify has lowest priority
        r = blockingQueue.poll();
        assertEquals(4, r.hashCode()); // Verify
    }
}
