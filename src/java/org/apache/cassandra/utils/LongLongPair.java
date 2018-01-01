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
package org.apache.cassandra.utils;

/**
 * Like org.apache.cassandra.utils.Pair, but specialized for <Long,Long> to avoid autoboxing
 */
public class LongLongPair
{
    public final long left;
    public final long right;

    protected LongLongPair(long left, long right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public final int hashCode()
    {
        int hashCode = (int) left ^ (int) (left >>> 32);
        return 31 * (hashCode ^ (int) ((int) right ^  (right >>> 32)));
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof LongLongPair))
            return false;
        LongLongPair that = (LongLongPair)o;
        return left == that.left && right == that.right;
    }

    @Override
    public String toString()
    {
        return "(" + left + ',' + right + ")";
    }

    public static LongLongPair create(long l, long r)
    {
        return new LongLongPair(l, r);
    }
}
