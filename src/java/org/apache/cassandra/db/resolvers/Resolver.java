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
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.index.SecondaryIndexManager;

import java.util.Iterator;

public interface Resolver {
    public String getName();

    public void writeTombstone(Row.Writer writer, ColumnDefinition column, long timestamp, int localDeletionTime);

    public Cell diff(Cell merged, Cell cell);

    public long reconcile(Clustering clustering,
                          Cell existing,
                          Cell update,
                          DeletionTime deletion,
                          Row.Writer writer,
                          int nowInSec,
                          SecondaryIndexManager.Updater indexUpdater);

    public Cell reconcile(Cell c1, Cell c2, int nowInSec);

    public long reconcileComplex(Clustering clustering,
                            ColumnDefinition column,
                            Iterator<Cell> existing,
                            Iterator<Cell> update,
                            DeletionTime deletion,
                            Row.Writer writer,
                            int nowInSec,
                            SecondaryIndexManager.Updater indexUpdater);


}
