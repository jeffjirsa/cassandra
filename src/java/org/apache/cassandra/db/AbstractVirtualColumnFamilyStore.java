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
package org.apache.cassandra.db;

import java.util.Map;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class AbstractVirtualColumnFamilyStore
{
    /**
     * @return the name of the column family
     */
    @Deprecated
    abstract public String getColumnFamilyName();

    abstract public String getTableName();
    
    /**
     * Register this class as a new virtual table
     */
    abstract public void create(String ksName, String cfName);

    /**
     * Is this table writable?
     *
     * @return True if UPDATE is supported
     */
    abstract public boolean writable();

    /*
     * Virtual table schemas aren't saved to disk, but we must have a way to describe them for drivers
     */
    abstract public Map<ColumnIdentifier, ColumnDefinition> getSchema();

    /**
     * Execute an update operation.
     *
     * @param partitionKey partition key for the update.
     * @param params parameters of the update.
     */
    public abstract void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException;

    /**
     * Perform a read on a virtual table
     *
     * @param state
     * @param options
     * @return
     * @throws RequestExecutionException
     * @throws RequestValidationException
     */
    public abstract ResultMessage.Rows execute(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException;

}
