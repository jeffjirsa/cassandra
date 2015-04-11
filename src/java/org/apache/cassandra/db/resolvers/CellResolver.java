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
package org.apache.cassandra.db.resolvers;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.AbstractCell;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.Cells;
import org.apache.cassandra.db.atoms.CellPath;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.tools.ant.taskdefs.Classloader;
import org.github.jamm.Unmetered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

@Unmetered
public abstract class CellResolver implements Resolver
{
    private final String className;

    public static final CounterContext counterContextManager = CounterContext.instance();
    private static final Logger logger = LoggerFactory.getLogger(TimestampResolver.class);

    protected CellResolver()
    {
        className = null;
    }

    public String getName()
    {
        return className;
    }
    /**
     * Writes a tombstone cell to the provided writer.
     *
     * @param writer the {@code Row.Writer} to write the tombstone to.
     * @param column the column for the tombstone.
     * @param timestamp the timestamp for the tombstone.
     * @param localDeletionTime the local deletion time (in seconds) for the tombstone.
     */
    public abstract void writeTombstone(Row.Writer writer, ColumnDefinition column, long timestamp, int localDeletionTime);

    /**
     * Computes the difference between a cell and the result of merging this
     * cell to other cells.
     * <p>
     * This method is used when cells from multiple sources are merged and we want to
     * find for a given source if it was up to date for that cell, and if not, what
     * should be sent to the source to repair it.
     *
     * @param merged the cell that is the result of merging multiple source.
     * @param cell the cell from one of the source that has been merged to yied
     * {@code merged}.
     * @return {@code null} if the source having {@code cell} is up-to-date for that
     * cell, or a cell that applied to the source will "repair" said source otherwise.
     */
    public abstract Cell diff(Cell merged, Cell cell);

    /**
     * Reconciles/merges two cells, one being an update to an existing cell,
     * yielding index updates if appropriate.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that those cells are for the same row and same
     * column (and same cell path if the column is complex).
     * <p>
     * Also note that which cell is provided as {@code existing} and which is
     * provided as {@code update} matters for index updates.
     *
     * @param clustering the clustering for the row the cells to merge originate from.
     * This is only used for index updates, so this can be {@code null} if
     * {@code indexUpdater == SecondaryIndexManager.nullUpdater}.
     * @param existing the pre-existing cell, the one that is updated. This can be
     * {@code null} if this reconciliation correspond to an insertion.
     * @param update the newly added cell, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy {@code existing} to
     * {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete both {@code existing} or {@code update}.
     * @param writer the row writer to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     * @param indexUpdater an index updater to which the result of the reconciliation is
     * signaled (if relevant, that is if the update is not simply ignored by the reconciliation).
     * This cannot be {@code null} but {@code SecondaryIndexManager.nullUpdater} can be passed.
     *
     * @return the timestamp delta between existing and update, or {@code Long.MAX_VALUE} if one
     * of them is {@code null} or deleted by {@code deletion}).
     */
    public abstract long reconcile(Clustering clustering,
                                 Cell existing,
                                 Cell update,
                                 DeletionTime deletion,
                                 Row.Writer writer,
                                 int nowInSec,
                                 SecondaryIndexManager.Updater indexUpdater);


    /**
     * Reconciles/merge two cells.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that cell are for the same row and same
     * column (and same cell path if the column is complex).
     * <p>
     * This method is commutative over it's cells arguments: {@code reconcile(a, b, n) == reconcile(b, a, n)}.
     *
     * @param c1 the first cell participating in the reconciliation.
     * @param c2 the second cell participating in the reconciliation.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     *
     * @return a cell corresponding to the reconciliation of {@code c1} and {@code c2}.
     * For non-counter cells, this will always be either {@code c1} or {@code c2}, but for
     * counter cells this can be a newly allocated cell.
     */
    public abstract Cell reconcile(Cell c1, Cell c2, int nowInSec);


    /**
     * Computes the reconciliation of a complex column given its pre-existing
     * cells and the ones it is updated with, and generating index update if
     * appropriate.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that the cells are for the same row and same
     * complex column.
     * <p>
     * Also note that which cells is provided as {@code existing} and which are
     * provided as {@code update} matters for index updates.
     *
     * @param clustering the clustering for the row the cells to merge originate from.
     * This is only used for index updates, so this can be {@code null} if
     * {@code indexUpdater == SecondaryIndexManager.nullUpdater}.
     * @param column the complex column the cells are for.
     * @param existing the pre-existing cells, the ones that are updated. This can be
     * {@code null} if this reconciliation correspond to an insertion.
     * @param update the newly added cells, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy the cells from
     * {@code existing} to {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete cells in both {@code existing} and {@code update}.
     * @param writer the row writer to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     * @param indexUpdater an index updater to which the result of the reconciliation is
     * signaled (if relevant, that is if the updates are not simply ignored by the reconciliation).
     * This cannot be {@code null} but {@code SecondaryIndexManager.nullUpdater} can be passed.
     *
     * @return the smallest timestamp delta between corresponding cells from existing and update. A
     * timestamp delta being computed as the difference between a cell from {@code update} and the
     * cell in {@code existing} having the same cell path (if such cell exists). If the intersection
     * of cells from {@code existing} and {@code update} having the same cell path is empty, this
     * returns {@code Long.MAX_VALUE}.
     */
    public abstract long reconcileComplex(Clustering clustering,
                                        ColumnDefinition column,
                                        Iterator<Cell> existing,
                                        Iterator<Cell> update,
                                        DeletionTime deletion,
                                        Row.Writer writer,
                                        int nowInSec,
                                        SecondaryIndexManager.Updater indexUpdater);


    public String toString()
    {
        return "CellResolver("+className+")";
    }

    public static Resolver getResolver(String className)
    {
        Resolver r;
        if (className == null)
            r = (Resolver) new TimestampResolver();
        else
        {
            try {
                r = (Resolver) Class.forName(className).newInstance();
            } catch (Exception e) {
                logger.warn(String.format("getResolver(%s) failed: %s , returning default timestamp resolver", className, e.getMessage()));
                r = (Resolver) new TimestampResolver();
            }
        }
        return r ;
    }

    /**
     * A simple implementation of a counter cell used when reconciling two counter cell
     * requires to allocate a new cell object (that is when the value of the reconciled
     * counter cells needs to be merged).
     */
    public static class SimpleCounter extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        public SimpleCounter(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
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