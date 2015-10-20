package org.apache.cassandra.db.resolvers;
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

import org.apache.cassandra.db.ConflictResolver;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.*;

public abstract class CellResolver implements ConflictResolver
{
    public String className;
    public List<AbstractType<?>> supportedTypes = new ArrayList<>();

    public String getName()
    {
        return "CellResolver";
    }

    public abstract ConflictResolver.Resolution resolveRegular(long leftTimestamp,
            boolean leftLive,
            int leftLocalDeletionTime,
            ByteBuffer leftValue,
            long rightTimestamp,
            boolean rightLive,
            int rightLocalDeletionTime,
            ByteBuffer rightValue);


    public static CellResolver getResolver()
    {
        return getResolver(null);
    }

    public static CellResolver getResolver(String className)
    {
        CellResolver r;
        if (className == null)
            r = new TimestampResolver();
        else
        {
            try {
                r = (CellResolver) Class.forName(className).newInstance();
            } catch (Exception e) {
                r = new TimestampResolver();
            }
        }

        return r ;
    }

    public Resolution resolveCounter(long leftTimestamp,
                                     boolean leftLive,
                                     ByteBuffer leftValue,
                                     long rightTimestamp,
                                     boolean rightLive,
                                     ByteBuffer rightValue)
    {
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (!leftLive)
            // left is a tombstone: it has precedence over right if either right is not a tombstone, or left has a greater timestamp
            return rightLive || leftTimestamp > rightTimestamp ? Resolution.LEFT_WINS : Resolution.RIGHT_WINS;

        // If right is a tombstone, since left isn't one, it has precedence
        if (!rightLive)
            return Resolution.RIGHT_WINS;

        return Resolution.MERGE;
    }

    public ByteBuffer mergeCounterValues(ByteBuffer left, ByteBuffer right)
    {
        return CounterContext.instance().merge(left, right);
    }

    public boolean supportsType(AbstractType<?> t)
    {
        return supportedTypes.contains(t);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(className);
    }

    @Override
    public boolean equals(Object o)
    {
        if(this == o)
            return true;

        return (o instanceof CellResolver && ((CellResolver) o).getName().equals(getName()));
    }

}
