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
import org.apache.cassandra.db.marshal.*;

import java.nio.ByteBuffer;
import java.util.*;

public class MinValueResolver extends CellResolver implements ConflictResolver
{
    public final String className = "org.apache.cassandra.db.resolvers.MinValueResolver";
    public final List<AbstractType<?>> supportedTypes = new ArrayList<AbstractType<?>>() {{
        add(DoubleType.instance);
        add(Int32Type.instance);
        add(LongType.instance);
        add(TimestampType.instance);
        add(AsciiType.instance);
        add(ByteType.instance);
        add(BytesType.instance);
        add(DateType.instance);
        add(FloatType.instance);
        add(IntegerType.instance);
        add(ShortType.instance);
        add(SimpleDateType.instance);
        add(TimeType.instance);
        add(TimeUUIDType.instance);
        add(UTF8Type.instance);
    }};

    public String getName()
    {
        return className;
    }

    public Resolution resolveRegular(long leftTimestamp,
                                            boolean leftLive,
                                            int leftLocalDeletionTime,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            int rightLocalDeletionTime,
                                            ByteBuffer rightValue)
    {
        if (!leftLive && leftTimestamp > rightTimestamp)
            return Resolution.LEFT_WINS;
        else if(!rightLive && rightTimestamp > leftTimestamp)
            return Resolution.RIGHT_WINS;
        else if (!leftLive)
            return Resolution.RIGHT_WINS;
        else if(!rightLive)
            return Resolution.LEFT_WINS;

        if (leftLive != rightLive)
            return leftLive ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;

        int c = leftValue.compareTo(rightValue);
        if (c < 0)
            return Resolution.LEFT_WINS;
        else if (c > 0)
            return Resolution.RIGHT_WINS;

        // Prefer the longest ttl if relevant
        return leftLocalDeletionTime < rightLocalDeletionTime ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;
    }

    public boolean supportsType(AbstractType<?> t)
    {
        return supportedTypes.contains(t);
    }


}
