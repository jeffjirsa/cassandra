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

import com.google.common.base.Objects;

/**
 * Like org.apache.cassandra.utils.Pair, but specialized for <T,Integer> to avoid autoboxing
 * the right (int) value
 */
public class ObjectIntPair<T>
{
    public final T left;
    public final int right;

    protected ObjectIntPair(T left, int right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public final int hashCode()
    {
        int hashCode = 31 + (left == null ? 0 : left.hashCode());
        return 31*hashCode * this.right;
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof ObjectIntPair))
            return false;
        ObjectIntPair that = (ObjectIntPair)o;
        // handles nulls properly
        return Objects.equal(left, that.left) && right == that.right;
    }

    @Override
    public String toString()
    {
        return "(" + left + ',' + right + ")";
    }

    public static <T> ObjectIntPair<T> create(T l, int r)
    {
        return new ObjectIntPair<>(l, r);
    }
}
