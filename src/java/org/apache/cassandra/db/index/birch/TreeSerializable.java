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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.composites.CType;

/**
 * Classes that implement TreeSerializable are responsible for
 * implementing all logic required to serialize a key and value
 * to add a given object to a Birch tree.
 *
 * As only the leaf nodes contain the values, TreeSerializable
 * objects provide methods for serializing the key and value
 * seperately. The Birch logic will serialize key and/or value
 * as required while building the tree.
 */
public interface TreeSerializable
{

    /**
     * @param type the Composite type to use for this key
     * @return the total serialized size (including any overhead from serialzation) of the key
     */
    int serializedKeySize(CType type);

    /**
     * @param type the Composite type to use for this key
     * @return a ByteBuffer containing the serialized bytes for this object's key
     */
    ByteBuffer serializedKey(CType type);

    /**
     * @return the total serialized size (including any overhead from serialzation) of the value
     */
    int serializedValueSize();

    /**
     * @param writer an instance of PageAlignedWriter to write the serialized version of the object
     * @throws IOException thrown when an IO exception is encountered while writing to the backing file
     */
    void serializeValue(PageAlignedWriter writer) throws IOException;
}
