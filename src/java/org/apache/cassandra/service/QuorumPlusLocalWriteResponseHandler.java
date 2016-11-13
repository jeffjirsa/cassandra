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
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.MessageIn;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class blocks for a quorum of responses cluster-wide as well as
 * a localConsistencyLevel in the local DC (ie: LOCAL_QUORUM)
 */
public class QuorumPlusLocalWriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    private final WriteResponseHandler<T> quorumHandler;
    private final DatacenterWriteResponseHandler<T> localQuorumHandler;

    public QuorumPlusLocalWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                               Collection<InetAddress> pendingEndpoints,
                                               ConsistencyLevel consistencyLevel,
                                               Keyspace keyspace,
                                               Runnable callback,
                                               WriteType writeType,
                                               ConsistencyLevel localConsistencyLevel)
    {
        super(keyspace, naturalEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);

        assert ((consistencyLevel == ConsistencyLevel.QUORUM_PLUS_LOCAL_QUORUM) ||
                (consistencyLevel == ConsistencyLevel.QUORUM_PLUS_LOCAL_ONE)) &&
                localConsistencyLevel.isDatacenterLocal();

        quorumHandler = new WriteResponseHandler<T>(naturalEndpoints, pendingEndpoints, ConsistencyLevel.QUORUM, keyspace, callback, writeType);
        localQuorumHandler = new DatacenterWriteResponseHandler<T>(naturalEndpoints, pendingEndpoints, localConsistencyLevel, keyspace, callback, writeType);
    }

    @Override
    public void response(MessageIn<T> message)
    {
        localQuorumHandler.response(message);
        quorumHandler.response(message);
    }

    @Override
    public void get() throws WriteTimeoutException, WriteFailureException
    {
        long requestTimeout = writeType == WriteType.COUNTER
                ? DatabaseDescriptor.getCounterWriteRpcTimeout()
                : DatabaseDescriptor.getWriteRpcTimeout();

        long timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - start);

        boolean localSuccess;
        boolean quorumSuccess;
        try
        {
            localSuccess = localQuorumHandler.condition.await(timeout, TimeUnit.NANOSECONDS);
            // Re-adjust timeout as needed
            timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - start);
            // SimpleCondition guarantees that calling await() after signal() returns immediately
            // so we don't need to worry about losing the signal() before we call await()
            quorumSuccess = localQuorumHandler.condition.await(timeout, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!(localSuccess && quorumSuccess))
        {
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new WriteTimeoutException(writeType, consistencyLevel, acks, blockedFor);
        }

        if (totalBlockFor() + failures > totalEndpoints())
        {
            throw new WriteFailureException(consistencyLevel, ackCount(), failures, totalBlockFor(), writeType);
        }
    }


    @Override
    protected int totalBlockFor()
    {
        return quorumHandler.totalBlockFor();
    }

    @Override
    protected int ackCount()
    {
        return quorumHandler.ackCount();
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
