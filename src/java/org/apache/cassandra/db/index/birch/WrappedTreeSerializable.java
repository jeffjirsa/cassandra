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

package org.apache.cassandra.db.index.birch;

/**
 * Class to wrap a {@link org.apache.cassandra.db.index.birch.TreeSerializable}
 * object with metadata used while building the tree, for instance keeping the
 * offset a given object was serialized out at so when promoted to inner
 * leaves we know what offset to encode into the tree so we can walk it when
 * reading the tree.
 *
 * @param <T> type of object being wrapped
 * @see org.apache.cassandra.db.index.birch.BirchWriter.Builder
 * @see TreeSerializable
 */
public class WrappedTreeSerializable<T>
{

    private final TreeSerializable treeSerializable;
    private long serializedOffset = 0;

    public WrappedTreeSerializable(TreeSerializable treeSerializable)
    {
        this.treeSerializable = treeSerializable;
    }

    public TreeSerializable getTreeSerializable()
    {
        return treeSerializable;
    }

    public long getSerializedOffset()
    {
        return serializedOffset;
    }

    public void setSerializedOffset(long serializedOffset)
    {
        this.serializedOffset = serializedOffset;
    }
}
