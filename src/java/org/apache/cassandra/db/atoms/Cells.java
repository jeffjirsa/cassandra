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
package org.apache.cassandra.db.atoms;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Static methods to work on cells.
 */
public abstract class Cells
{
    public static final CounterContext counterContextManager = CounterContext.instance();

    private Cells() {}

    /**
     * Writes a tombstone cell to the provided writer.
     *
     * @param writer the {@code Row.Writer} to write the tombstone to.
     * @param column the column for the tombstone.
     * @param timestamp the timestamp for the tombstone.
     * @param localDeletionTime the local deletion time (in seconds) for the tombstone.
     */
    public static void writeTombstone(Row.Writer writer, ColumnDefinition column, long timestamp, int localDeletionTime)
    {
        writer.writeCell(column, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, SimpleLivenessInfo.forDeletion(timestamp, localDeletionTime), null);
    }

    private static Cell getNext(Iterator<Cell> iterator)
    {
        return iterator == null || !iterator.hasNext() ? null : iterator.next();
    }

    /**
     * A simple implementation of a counter cell used when reconciling two counter cell
     * requires to allocate a new cell object (that is when the value of the reconciled
     * counter cells needs to be merged).
     */
    private static class SimpleCounter extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        private SimpleCounter(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            this.column = column;
            this.value = value;
            this.info = info.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return true;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return null;
        }
    }
}
