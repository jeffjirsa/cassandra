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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.sstable.IndexInfo;

/**
 * <strong>1. BirchWriter Tree Serialization Format</strong>
 * <p>
 * <strong>1.1. Overview</strong>
 * <p>
 * BirchWriter builds and serializes a Birch Tree. The output from BirchWriter
 * can be read by {@link org.apache.cassandra.db.index.birch.BirchReader}.
 * It is backed by a {@link org.apache.cassandra.db.index.birch.PageAlignedWriter}.
 * The name "Birch" comes from a play on words, where "Birch" is a type
 * of tree that begins with the letter 'b'. :)
 * <p>
 * A BirchWriter Tree is a page aligned B+ish on-disk tree structure. Each
 * tree is made of n {@link org.apache.cassandra.db.index.birch.Node}
 * (see section 1.2) (leaf and inner nodes), and an encoded
 * {@link org.apache.cassandra.db.index.birch.Overflow} segment (optionally
 * encoded to handle elements inserted into the tree with keys that do not
 * fit within a node), and finally a tree descriptor.
 * <p>
 * <pre>
 * {@code
 *                  1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3
 *    1 2 3 5 6 7 8 0 1 2 4 5 6 7 9 0 1 3 4 5 6 8 9 0 2 3 4 5 7 8 9
 *    2 5 8 1 4 6 9 2 5 8 0 3 6 9 2 4 7 0 3 6 8 1 4 7 0 2 5 8 1 4 6
 *  0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8 6 4 2 0 8
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                     BirchWriter Node 1 (leaf)                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                     BirchWriter Node 2 (leaf)                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    BirchWriter Node 3 (inner)                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Overflow (optional)                        |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Descriptor                            ||
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 * <p>
 *
 * <strong>1.2. BirchWriter Node Overview</strong>
 * <p>
 * <pre>
 * {@code
 *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |       Number of Elements      |        start offset (e1)      |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |        start offset (e2)      |        start offset (e3)      |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         end offset (e3)       |            value (e1)         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |           value (e2)          |            value (e3)         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  has overflow |                key (e1)                       /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /         |                  key (e2)                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                 key overflow page offset (e2)                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                            key (e3)                     ||    /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /                                                               /
 * /              padded to next end of page boundary              /
 * /                                                               /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 * <p>
 *
 * <strong>1.3. BirchWriter Node Description</strong>
 * <p>
 * Each node/leaf in the tree is serialized as follows:
 * <ul>
 *     <li>[short] * Number of elements encoded in node (henceforth "n")
 *     <li>[short] * (n+1) * The starting offset into this node that contains the bytes
 *                  for each element's key [o1][o2][o..n] and [o..n+1] is the
 *                  ending offset of the last element
 *     <li>[short] * (n) * The value for each element (which is of short length in
 *                  this example)
 *     <li>[byte] * ceil(n/8) * A bit-field encoding which element's keys use the
 *                  {@link org.apache.cassandra.db.index.birch.Overflow} page.
 *     <li>[k bytes] * (n) * The serialized element's key. If the key exceeds the max key
 *                  length (1Kb by default), the [int] offset into the overflow page
 *                  that contains the rest of the key's bytes.
 * </ul>
 * <p>
 * If all of the above does not fill the page (4Kb by default)
 * exactly (almost certainly always the case) the rest of the
 * page is padded from the end of the last key's bytes to the page's boundary.
 * <p>
 * The above diagram shows what the serialized bytes would look like for a
 * {@link org.apache.cassandra.db.index.birch.Node} with 3 elements, a 16-byte [short]
 * value, where element 2 exceeds the max key size and has the offset for it's
 * {@link org.apache.cassandra.db.index.birch.Overflow} component encoded.
 * <p>
 *
 * <strong>2. Serialization</strong>
 * <p>
 * <strong>2.1. Reading from the tree</strong>
 * <p>
 * As the fixed length components are serialized together separate from
 * the variable length key component of each element, we can deterministically
 * iterate or binary search over the n serialized offsets. As the total size
 * of the encoded offsets is deterministic and fixed based on the number of
 * elements encoded we can multiply the size of each offset
 * [short] * [num elements] + [short] to get the starting offset of the values
 * (which are also of fixed length). The value for each element will be at the
 * starting offset of the values + (the size of a single value * the current index)
 * of a given offset. As the fixed components are in front we can calculate the
 * total length of those to get the starting offset of the first key. The length
 * of a given element's key can be calculated by taking the offset at i+1 - the
 * offset at i. This means we do not need to encode the length and offset twice
 * and save 16-bytes [short] per element. We need to, however, encode the nth
 * elements ending offset (and incur a 16-byte single [short] cost) to be able
 * to calculate the final nth element's length.
 * <p>
 * With the data serialized this way we can 1) iterate over all elements in both
 * forwards and reversed order
 * (see {@link org.apache.cassandra.db.index.birch.BirchReader.BirchIterator}),
 * 2) binary search for a specific element
 * (see {@link org.apache.cassandra.db.index.birch.BirchReader#search(Composite, CType, boolean)}),
 * 3) efficiently encode variable length keys (with support for keys that exceed
 * the length that can be encoded inside the node itself), and finally 4) incur
 * the minimum possible serialization overhead by serializing in a way that enables
 * safe assumptions to be made by the deserialization code (and avoiding additional
 * serialization overhead).
 *
 * @see BirchReader
 * @see PageAlignedWriter
 * @see PageAlignedReader
 * @see Node
 * @see Overflow
 */
public class BirchWriter<T>
{
    private static final short VERSION_1_0 = 1;
    public static final short CURRENT_VERSION = VERSION_1_0;

    private final Iterator<TreeSerializable> elementsToAddIterator;
    private final int cacheLineSize;
    private final short maxKeyLengthWithoutOverflow;
    private final BSerializer serializer;
    private final CType type;

    public enum SerializerType
    {
        UNKNOWN(0),
        INDEXINFO(1);

        private final int id;

        SerializerType(int id)
        {
            this.id = id;
        }

        public int getId()
        {
            return id;
        }

        public static SerializerType getById(int id)
        {
            for (SerializerType e : values())
            {
                if (e.id == id)
                {
                    return e;
                }
            }
            return UNKNOWN;
        }
    }

    public static final EnumMap<SerializerType, BSerializer> SERIALIZERS = new EnumMap<SerializerType, BSerializer>(SerializerType.class)
    {{
        put(SerializerType.INDEXINFO, IndexInfo.SERIALIZER);
    }};

    private BirchWriter(Builder<T> builder)
    {
        this.elementsToAddIterator = builder.elms;
        this.cacheLineSize = builder.cacheLineSize;
        this.maxKeyLengthWithoutOverflow = builder.maxKeyLengthWithoutOverflow;
        this.serializer = SERIALIZERS.get(builder.serializerType);
        this.type = builder.type;
    }

    public void serialize(PageAlignedWriter writer) throws IOException
    {
        Descriptor.Builder descriptorBuilder = new Descriptor.Builder(cacheLineSize, serializer.getType());
        Overflow overflow = new Overflow(writer.getPath());

        List<Node> leafNodes = new ArrayList<>();
        Node inProgressLeaf = new Node(type, serializer, maxKeyLengthWithoutOverflow, cacheLineSize, overflow);

        descriptorBuilder = descriptorBuilder.firstLeafOffset(writer.getFilePointer());

        int elementsAdded = 0;
        while (elementsToAddIterator.hasNext())
        {
            TreeSerializable elm = elementsToAddIterator.next();
            if (!inProgressLeaf.hasCapacity(elm))
            {
                inProgressLeaf.serialize(writer);
                inProgressLeaf.trimElements();
                leafNodes.add(inProgressLeaf);

                // create a the next leaf
                inProgressLeaf = new Node(type, serializer, maxKeyLengthWithoutOverflow, cacheLineSize, overflow);
            }

            inProgressLeaf.add(elm);
            elementsAdded++;
        }

        // add the last in progress leaf (if it actually contains at least 1 element)
        if (inProgressLeaf.getNumberOfElements() > 0)
        {
            inProgressLeaf.serialize(writer);
            leafNodes.add(inProgressLeaf);
        }

        List<List<Node>> innerNodes = new ArrayList<>();

        List<Node> sourceLevel = leafNodes;
        List<Node> inProgressInnerNodeLevel = new ArrayList<>();
        while (inProgressInnerNodeLevel.isEmpty() || inProgressInnerNodeLevel.size() > 1)
        {
            Node inProgressInnerNode = new Node(type, serializer, maxKeyLengthWithoutOverflow, cacheLineSize, overflow);
            for (Node node : sourceLevel)
            {
                node.getElementToPromote().getTreeSerializable().serializedKey(type);

                if (!inProgressInnerNode.hasCapacity(node.getElementToPromote().getTreeSerializable()))
                {
                    inProgressInnerNodeLevel.add(inProgressInnerNode);

                    // create next inner node
                    inProgressInnerNode = new Node(type, serializer, maxKeyLengthWithoutOverflow, cacheLineSize, overflow);
                }
                inProgressInnerNode.add(node);
            }

            if (inProgressInnerNode.getNumberOfElements() > 0)
            {
                inProgressInnerNodeLevel.add(inProgressInnerNode);
            }

            if (inProgressInnerNodeLevel.isEmpty())
            {
                break;
            }

            innerNodes.add(inProgressInnerNodeLevel);
            if (inProgressInnerNodeLevel.size() > 1)
            {
                sourceLevel = inProgressInnerNodeLevel;
                inProgressInnerNodeLevel = new ArrayList<>();
            }
            else
            {
                break;
            }
        }

        descriptorBuilder = descriptorBuilder.firstNodeOffset(writer.getFilePointer());

        for (List<Node> nodes : innerNodes)
        {
            if (nodes.size() == 1)
            {
                // encode the root (and final) node
                descriptorBuilder = descriptorBuilder.rootOffset(writer.getFilePointer());
            }

            for (Node promotedNode : nodes)
            {
                promotedNode.serialize(writer);
            }
        }

        long overflowPageOffset = writer.getFilePointer();
        long overflowPageLength = overflow.serialize(writer);
        if (overflowPageLength > 0) // we did serialize out an overflow component
        {
            descriptorBuilder = descriptorBuilder.overflowPageOffset(overflowPageOffset).overflowPageLength(overflowPageLength);
            long alignTo = writer.getNextAlignedOffset(writer.getCurrentFilePosition());
            // although we don't pad/chunk the overflow page internally, we do want to make sure
            // we pad the end of it out to an aligned boundary
            writer.seek(alignTo + cacheLineSize);
        }
        overflow.close();

        // The last thing left is to serialize out the tree descriptor at the very end.
        // The reader will make the assumption it can can go to the end of the segment and
        // skip back a fixed descriptor's worth of bytes
        Descriptor descriptor = descriptorBuilder.elementCount(elementsAdded).build();
        descriptor.serialize(writer);
    }

    public static class Builder<T>
    {
        private final Iterator<TreeSerializable> elms;
        private final SerializerType serializerType;
        private final CType type;
        private int cacheLineSize = DatabaseDescriptor.getSSTableIndexSegmentPaddingLength();
        private short maxKeyLengthWithoutOverflow = (short) ((cacheLineSize / 2) / 2);

        public Builder(Iterator elms, SerializerType serializerType, CType type)
        {
            this.elms = elms;
            this.serializerType = serializerType;
            this.type = type;
        }

        public Builder cacheLineSize(int size)
        {
            this.cacheLineSize = size;
            return this;
        }

        public Builder maxKeyLengthWithoutOverflow(short size)
        {
            this.maxKeyLengthWithoutOverflow = size;
            return this;
        }

        public BirchWriter<T> build()
        {
            return new BirchWriter<>(this);
        }
    }

    public int getCacheLineSize()
    {
        return cacheLineSize;
    }
}
