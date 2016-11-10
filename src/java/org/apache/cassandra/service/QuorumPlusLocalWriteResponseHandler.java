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
package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.MessageIn;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * This class blocks for a quorum of responses AND quorum in the local DC  (CL.QUORUM_PLUS_LOCAL).
 */
public class QuorumPlusLocalWriteResponseHandler<T> extends WriteResponseHandler<T>
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private volatile int localResponses;
    private static final AtomicIntegerFieldUpdater<QuorumPlusLocalWriteResponseHandler> localResponsesUpdater
            = AtomicIntegerFieldUpdater.newUpdater(QuorumPlusLocalWriteResponseHandler.class, "localResponses");

    public QuorumPlusLocalWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                               Collection<InetAddress> pendingEndpoints,
                                               ConsistencyLevel consistencyLevel,
                                               Keyspace keyspace,
                                               Runnable callback,
                                               WriteType writeType)
    {
        super(naturalEndpoints, pendingEndpoints, consistencyLevel, keyspace, callback, writeType);

        assert consistencyLevel == ConsistencyLevel.QUORUM_PLUS_LOCAL;

        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        int localRf = strategy.getReplicationFactor(DatabaseDescriptor.getLocalDataCenter());
        localResponses = (( localRf / 2) + 1 );

        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        for (InetAddress pending : pendingEndpoints)
        {
            if(snitch.getDatacenter(pending).equals(DatabaseDescriptor.getLocalDataCenter()))
                localResponsesUpdater.incrementAndGet(this);
        }
    }

    @Override
    public void response(MessageIn<T> message)
    {
        String dataCenter = message == null
                            ? DatabaseDescriptor.getLocalDataCenter()
                            : snitch.getDatacenter(message.from);


        if(dataCenter.equals(DatabaseDescriptor.getLocalDataCenter()))
            localResponsesUpdater.decrementAndGet(this);

        if(responsesUpdater.decrementAndGet(this) <= 0 && localResponsesUpdater.get(this) <= 0 )
            signal();
    }
}
