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

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class BufferExpiringCounterCell extends BufferCell implements ExpiringCounterCell
{
    private final long timestampOfLastDelete;
    private final int localExpirationTime;
    private final int timeToLive;


    public BufferExpiringCounterCell(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete, int timeToLive)
    {
        this(name, value, timestamp, Long.MIN_VALUE, timeToLive, (int) (System.currentTimeMillis() / 1000) + timeToLive);
    }

    public BufferExpiringCounterCell(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete, int timeToLive, int localExpirationTime)
    {
        super(name, value, timestamp);
        // For the counter component
        this.timestampOfLastDelete = timestampOfLastDelete;
        // For the expiring component
        assert timeToLive > 0 : timeToLive;
        assert localExpirationTime > 0 : localExpirationTime;
        this.timeToLive = timeToLive;
        this.localExpirationTime = localExpirationTime;

    }

    public static Cell create(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete, int timeToLive, int localExpirationTime, int expireBefore, ColumnSerializer.Flag flag)
    {
        if (flag == ColumnSerializer.Flag.FROM_REMOTE || (flag == ColumnSerializer.Flag.LOCAL && contextManager.shouldClearLocal(value)))
            value = contextManager.clearAllLocal(value);
        
        if (localExpirationTime >= expireBefore || flag == ColumnSerializer.Flag.PRESERVE_SIZE)
            return new BufferExpiringCounterCell(name, value, timestamp, timestampOfLastDelete, timeToLive, localExpirationTime);

        // The column is now expired, we can safely return a simple tombstone. Note that
        // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
        // we'll fulfil our responsibility to repair.  See discussion at
        // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
        return new BufferDeletedCell(name, localExpirationTime - timeToLive, timestamp);
    }

    // For use by tests of compatibility with pre-2.1 counter only.
    public static ExpiringCounterCell createLocal(CellName name, long value, long timestamp, long timestampOfLastDelete, int timeToLive, int localExpirationTime)
    {
        return new BufferExpiringCounterCell(name, contextManager.createLocal(value), timestamp, timestampOfLastDelete, timeToLive, localExpirationTime);
    }
    
    public int getTimeToLive()
    {
        return timeToLive;
    }


    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferExpiringCounterCell(newName, value, timestamp, timestampOfLastDelete, timeToLive, localExpirationTime);
    }

    @Override
    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    @Override
    public long total()
    {
        return contextManager.total(value);
    }

    @Override
    public int cellDataSize()
    {
        // A counter column adds 8 bytes for timestampOfLastDelete to Cell.
        return super.cellDataSize() + TypeSizes.NATIVE.sizeof(timestampOfLastDelete);
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return super.serializedSize(type, typeSizes) + typeSizes.sizeof(timestampOfLastDelete) + typeSizes.sizeof(localExpirationTime) + typeSizes.sizeof(timeToLive);
    }

    @Override
    public Cell diff(Cell cell)
    {
        return diffCounter(cell);
    }

    /*
     * We have to special case digest creation for counter column because
     * we don't want to include the information about which shard of the
     * context is a delta or not, since this information differs from node to
     * node.
     */
    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name().toByteBuffer().duplicate());
        // We don't take the deltas into account in a digest
        contextManager.updateDigest(digest, value());

        FBUtilities.updateWithLong(digest, timestamp);
        FBUtilities.updateWithByte(digest, serializationFlags());
        FBUtilities.updateWithLong(digest, timestampOfLastDelete);
        FBUtilities.updateWithInt(digest, timeToLive);

    }

    @Override
    public Cell reconcile(Cell cell)
    {
        return reconcileExpiringCounter(cell);
    }

    @Override
    public boolean hasLegacyShards()
    {
        return contextManager.hasLegacyShards(value);
    }

    @Override
    public ExpiringCounterCell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferExpiringCounterCell(name.copy(metadata, allocator), allocator.clone(value), timestamp, timestampOfLastDelete, timeToLive, localExpirationTime);
    }

    @Override
    public ExpiringCounterCell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        return allocator.clone(this, metadata, opGroup);
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s:false:%s@%d!%d",
                             comparator.getString(name()),
                             contextManager.toString(value()),
                             timestamp(),
                             timestampOfLastDelete);
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.EXPIRATION_MASK | ColumnSerializer.COUNTER_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
        // which is not the internal representation of counters
        contextManager.validateContext(value());
    }

    @Override
    public Cell markLocalToBeCleared()
    {
        ByteBuffer marked = contextManager.markLocalToBeCleared(value());
        return marked == value() ? this : new BufferExpiringCounterCell(name(), marked, timestamp(), timestampOfLastDelete, timeToLive, localExpirationTime);
    }

    @Override
    public boolean equals(Cell cell)
    {
        return cell instanceof ExpiringCounterCell && equals((ExpiringCounterCell) cell);
    }

    public boolean equals(ExpiringCounterCell cell)
    {
        return super.equals(cell) && 
        		timestampOfLastDelete == cell.timestampOfLastDelete() && 
        		getLocalDeletionTime() == cell.getLocalDeletionTime() && 
        		getTimeToLive() == cell.getTimeToLive();
    }
    
    @Override
    public int getLocalDeletionTime()
    {
        return localExpirationTime;
    }


    @Override
    public boolean isLive()
    {
        return isLive(System.currentTimeMillis());
    }

    @Override
    public boolean isLive(long now)
    {
        return (int) (now / 1000) < getLocalDeletionTime();
    }


}
