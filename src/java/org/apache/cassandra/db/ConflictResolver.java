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

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

public interface ConflictResolver
{
    public String getName();

    public enum Resolution { LEFT_WINS, MERGE, RIGHT_WINS };

    public Resolution resolveRegular(long leftTimestamp,
                                            boolean leftLive,
                                            int leftLocalDeletionTime,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            int rightLocalDeletionTime,
                                            ByteBuffer rightValue);

    public Resolution resolveCounter(long leftTimestamp,
                                            boolean leftLive,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            ByteBuffer rightValue);

    public ByteBuffer mergeCounterValues(ByteBuffer left, ByteBuffer right);

    public boolean supportsType(AbstractType<?> t);

}
