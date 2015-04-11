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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    private static final Logger logger = LoggerFactory.getLogger(Rows.class);

    private Rows() {}

    public static final Row EMPTY_STATIC_ROW = new AbstractRow()
    {
        public Columns columns()
        {
            return Columns.NONE;
        }

        public LivenessInfo partitionKeyLivenessInfo()
        {
            return LivenessInfo.NONE;
        }

        public long maxLiveTimestamp()
        {
            return LivenessInfo.NO_TIMESTAMP;
        }

        public int nowInSec()
        {
            return FBUtilities.nowInSeconds();
        }

        public DeletionTime deletion()
        {
            return DeletionTime.LIVE;
        }

        public boolean isEmpty()
        {
            return true;
        }

        public boolean hasComplexDeletion()
        {
            return false;
        }

        public Clustering clustering()
        {
            return Clustering.STATIC_CLUSTERING;
        }

        public Cell getCell(ColumnDefinition c)
        {
            return null;
        }

        public Cell getCell(ColumnDefinition c, CellPath path)
        {
            return null;
        }

        public Iterator<Cell> getCells(ColumnDefinition c)
        {
            return null;
        }

        public DeletionTime getDeletion(ColumnDefinition c)
        {
            return DeletionTime.LIVE;
        }

        public Iterator<Cell> iterator()
        {
            return Iterators.<Cell>emptyIterator();
        }

        public Kind kind()
        {
            return Atom.Kind.ROW;
        }

        public Row takeAlias()
        {
            return this;
        }
    };

    public interface SimpleMergeListener
    {
        public void onAdded(Cell newCell);
        public void onRemoved(Cell removedCell);
        public void onUpdated(Cell existingCell, Cell updatedCell);
    }

    public static void writeClustering(Clustering clustering, Row.Writer writer)
    {
        for (int i = 0; i < clustering.size(); i++)
            writer.writeClusteringValue(clustering.get(i));
    }

    // Merge rows in memtable
    // Return the minimum timestamp delta between existing and update
    public static long merge(Row existing,
                             Row update,
                             Columns mergedColumns,
                             Row.Writer writer,
                             int nowInSec,
                             SecondaryIndexManager.Updater indexUpdater)
    {
        Clustering clustering = existing.clustering();
        writeClustering(clustering, writer);

        LivenessInfo existingInfo = existing.partitionKeyLivenessInfo();
        LivenessInfo updateInfo = update.partitionKeyLivenessInfo();
        LivenessInfo mergedInfo = existingInfo.mergeWith(updateInfo);

        long timeDelta = Math.abs(existingInfo.timestamp() - mergedInfo.timestamp());

        DeletionTime deletion = existing.deletion().supersedes(update.deletion()) ? existing.deletion() : update.deletion();

        if (deletion.deletes(mergedInfo))
            mergedInfo = LivenessInfo.NONE;

        writer.writePartitionKeyLivenessInfo(mergedInfo);
        writer.writeRowDeletion(deletion);

        long maxLiveTimestamp = Math.max(existing.maxLiveTimestamp(), update.maxLiveTimestamp());
        writer.writeMaxLiveTimestamp(maxLiveTimestamp);

        indexUpdater.maybeIndex(clustering, maxLiveTimestamp, mergedInfo.ttl(), deletion);

        for (int i = 0; i < mergedColumns.simpleColumnCount(); i++)
        {
            // XXX - if it's a UDT, look for custom resolver, and call the UDTs custom resolver
            ColumnDefinition c = mergedColumns.getSimple(i);
            Cell existingCell = existing.getCell(c);
            Cell updateCell = update.getCell(c);
            timeDelta = Math.min(timeDelta, c.getResolver().reconcile(clustering,
                    existingCell,
                    updateCell,
                    deletion,
                    writer,
                    nowInSec,
                    indexUpdater));
        }

        for (int i = 0; i < mergedColumns.complexColumnCount(); i++)
        {
            ColumnDefinition c = mergedColumns.getComplex(i);
            DeletionTime existingDt = existing.getDeletion(c);
            DeletionTime updateDt = update.getDeletion(c);
            DeletionTime maxDt = existingDt.supersedes(updateDt) ? existingDt : updateDt;
            if (maxDt.supersedes(deletion))
                writer.writeComplexDeletion(c, maxDt);
            else
                maxDt = deletion;

            Iterator<Cell> existingCells = existing.getCells(c);
            Iterator<Cell> updateCells = update.getCells(c);
            timeDelta = Math.min(timeDelta, c.getResolver().reconcileComplex(clustering, c, existingCells, updateCells, maxDt, writer, nowInSec, indexUpdater));
        }

        writer.endOfRow();
        return timeDelta;
    }

    public static long computeMaxLiveTimestamp(Row row)
    {
        long maxLive = LivenessInfo.NO_TIMESTAMP;
        if (row.partitionKeyLivenessInfo().isLive(row.nowInSec()))
            maxLive = row.partitionKeyLivenessInfo().timestamp();

        for (Cell cell : row)
            if (cell.isLive(row.nowInSec()) && cell.livenessInfo().timestamp() > maxLive)
                maxLive = cell.livenessInfo().timestamp();
        return maxLive;
    }

    public abstract static class Merger
    {
        private final CFMetaData metadata;
        private final int nowInSec;
        private final AtomIterators.MergeListener listener;
        private final Columns columns;

        private Clustering clustering;
        private final Row[] rows;
        private int rowsToMerge;

        private final Cell[] cells;
        private final List<Iterator<Cell>> complexCells;
        private final ComplexColumnReducer complexReducer = new ComplexColumnReducer();

        // For the sake of the listener if there is one
        private final DeletionTime[] complexDelTimes;

        public static Merger createStatic(CFMetaData metadata, int size, int nowInSec, Columns columns, AtomIterators.MergeListener listener)
        {
            return new StaticMerger(metadata, size, nowInSec, columns, listener);
        }

        public static Merger createRegular(CFMetaData metadata, int size, int nowInSec, Columns columns, AtomIterators.MergeListener listener)
        {
            return new RegularMerger(metadata, size, nowInSec, columns, listener);
        }

        protected Merger(CFMetaData metadata, int size, int nowInSec, Columns columns, AtomIterators.MergeListener listener)
        {
            this.metadata = metadata;
            this.nowInSec = nowInSec;
            this.listener = listener;
            this.columns = columns;
            this.rows = new Row[size];
            this.complexCells = new ArrayList<>(size);

            this.cells = new Cell[size];
            this.complexDelTimes = listener == null ? null : new DeletionTime[size];
        }

        public void clear()
        {
            Arrays.fill(rows, null);
            Arrays.fill(cells, null);
            if (complexDelTimes != null)
                Arrays.fill(complexDelTimes, null);
            complexCells.clear();
            rowsToMerge = 0;
        }

        public void add(int i, Row row)
        {
            clustering = row.clustering();
            rows[i] = row;
            ++rowsToMerge;
        }

        protected abstract Row.Writer getWriter();
        protected abstract Row getRow();

        public Row merge(DeletionTime activeDeletion)
        {

            // If for this clustering we have only one row version and have no activeDeletion (i.e. nothing to filter out),
            // then we can just return that single row (we also should have no listener)
            if (rowsToMerge == 1 && activeDeletion.isLive() && listener == null)
            {
                for (int i = 0; i < rows.length; i++)
                    if (rows[i] != null)
                        return rows[i];

                throw new AssertionError();
            }

            Row.Writer writer = getWriter();
            writeClustering(clustering, writer);

            LivenessInfo info = LivenessInfo.NONE;
            long maxLiveTimestamp = LivenessInfo.NO_TIMESTAMP;
            DeletionTime deletion = DeletionTime.LIVE;
            for (int i = 0; i < rows.length; i++)
            {
                if (rows[i] == null)
                    continue;

                info = info.mergeWith(rows[i].partitionKeyLivenessInfo());

                if (rows[i].maxLiveTimestamp() > maxLiveTimestamp)
                    maxLiveTimestamp = rows[i].maxLiveTimestamp();

                if (rows[i].deletion().supersedes(deletion))
                    deletion = rows[i].deletion();
            }

            if (deletion.supersedes(activeDeletion))
                activeDeletion = deletion;

            if (activeDeletion.deletes(info))
                info = LivenessInfo.NONE;

            if (activeDeletion.deletes(maxLiveTimestamp))
                maxLiveTimestamp = LivenessInfo.NO_TIMESTAMP;

            writer.writePartitionKeyLivenessInfo(info);
            writer.writeRowDeletion(deletion);
            writer.writeMaxLiveTimestamp(maxLiveTimestamp);

            if (listener != null)
                listener.onMergingRows(clustering, info, deletion, rows);

            for (int i = 0; i < columns.simpleColumnCount(); i++)
            {
                ColumnDefinition c = columns.getSimple(i);
                for (int j = 0; j < rows.length; j++)
                    cells[j] = rows[j] == null ? null : rows[j].getCell(c);

                reconcileCells(activeDeletion, c, writer);
            }

            complexReducer.activeDeletion = activeDeletion;
            complexReducer.writer = writer;
            for (int i = 0; i < columns.complexColumnCount(); i++)
            {
                ColumnDefinition c = columns.getComplex(i);

                // logger.warn("Merge complex columns, merging column " + c.toString() + " using resolver " + c.getResolver().toString());

                DeletionTime maxComplexDeletion = DeletionTime.LIVE;
                for (int j = 0; j < rows.length; j++)
                {
                    if (rows[j] == null)
                        continue;

                    DeletionTime dt = rows[j].getDeletion(c);
                    if (complexDelTimes != null)
                        complexDelTimes[j] = dt;

                    if (dt.supersedes(maxComplexDeletion))
                        maxComplexDeletion = dt;
                }

                boolean overrideActive = maxComplexDeletion.supersedes(activeDeletion);
                maxComplexDeletion =  overrideActive ? maxComplexDeletion : DeletionTime.LIVE;
                writer.writeComplexDeletion(c, maxComplexDeletion);
                if (listener != null)
                    listener.onMergedComplexDeletion(c, maxComplexDeletion, complexDelTimes);

                mergeComplex(overrideActive ? maxComplexDeletion : activeDeletion, c);
            }
            writer.endOfRow();
            if (listener != null)
                listener.onRowDone();

            // Because shadowed cells are skipped, the row could be empty. In which case
            // we return null.
            Row row = getRow();
            return row.isEmpty() ? null : row;
        }

        private void reconcileCells(DeletionTime activeDeletion, ColumnDefinition c, Row.Writer writer)
        {
            Cell reconciled = null;
            for (int j = 0; j < cells.length; j++)
            {
                Cell cell = cells[j];
                if (cell != null && !activeDeletion.deletes(cell.livenessInfo()))
                    reconciled = c.getResolver().reconcile(reconciled, cell, nowInSec);
            }

            if (reconciled != null)
            {
                reconciled.writeTo(writer);
                if (listener != null)
                    listener.onMergedCells(reconciled, cells);
            }
        }

        private void mergeComplex(DeletionTime activeDeletion, ColumnDefinition c)
        {
            complexCells.clear();
            for (int j = 0; j < rows.length; j++)
            {
                Row row = rows[j];
                Iterator<Cell> iter = row == null ? null : row.getCells(c);
                complexCells.add(iter == null ? Iterators.<Cell>emptyIterator() : iter);
            }

            complexReducer.column = c;
            complexReducer.activeDeletion = activeDeletion;

            // Note that we use the mergeIterator only to group cells to merge, but we
            // write the result to the writer directly in the reducer, so all we care
            // about is iterating over the result.
            Iterator<Void> iter = MergeIterator.get(complexCells, c.cellComparator(), complexReducer);
            while (iter.hasNext())
                iter.next();
        }

        private class ComplexColumnReducer extends MergeIterator.Reducer<Cell, Void>
        {
            private DeletionTime activeDeletion;
            private Row.Writer writer;
            private ColumnDefinition column;

            public void reduce(int idx, Cell current)
            {
                cells[idx] = current;
            }

            protected Void getReduced()
            {
                reconcileCells(activeDeletion, column, writer);
                return null;
            }

            protected void onKeyChange()
            {
                Arrays.fill(cells, null);
            }
        }

        private static class StaticMerger extends Merger
        {
            private final StaticRow.Builder builder;

            private StaticMerger(CFMetaData metadata, int size, int nowInSec, Columns columns, AtomIterators.MergeListener listener)
            {
                super(metadata, size, nowInSec, columns, listener);
                this.builder = StaticRow.builder(columns, nowInSec);
            }

            protected Row.Writer getWriter()
            {
                return builder;
            }

            protected Row getRow()
            {
                return builder.build();
            }
        }

        private static class RegularMerger extends Merger
        {
            private final ReusableRow row;

            private RegularMerger(CFMetaData metadata, int size, int nowInSec, Columns columns, AtomIterators.MergeListener listener)
            {
                super(metadata, size, nowInSec, columns, listener);
                this.row = new ReusableRow(metadata.clusteringColumns().size(), columns, nowInSec);
            }

            protected Row.Writer getWriter()
            {
                return row.writer();
            }

            protected Row getRow()
            {
                return row;
            }
        }
    }
}
