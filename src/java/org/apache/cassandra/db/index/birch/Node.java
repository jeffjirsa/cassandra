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

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of both leaf and inner nodes to be added to a Birch tree.
 *
 * @see BirchWriter
 */
public class Node implements TreeSerializable
{
    private final short maxKeyLength;
    private final int pageSize;
    private final BSerializer serializer;
    private final CType type;
    private final Overflow overflow;
    private List<WrappedTreeSerializable> elements;

    private int sizeOfAddedElements = Short.BYTES; // always start at 0+Short.BYTES to accommodate encoding number of elements

    public Node(CType type, BSerializer serializer, short maxKeyLength, int pageSize, Overflow overflow)
    {
        this.type = type;
        this.serializer = serializer;
        this.maxKeyLength = maxKeyLength;
        this.pageSize = pageSize;
        this.overflow = overflow;
        this.elements = new ArrayList<>();
    }

    public boolean hasCapacity(TreeSerializable elm)
    {
        // we need to calculate this each time as the size of the
        // field is variable length (and is sized to fit the number of elements)
        int hasOffsetFieldSize = bytesRequiredForHasOverflowField();
        return sizeOfAddedElements + hasOffsetFieldSize + serializedLength(elm) < pageSize;
    }

    public void trimElements()
    {
        List<WrappedTreeSerializable> trimmedElements = new ArrayList<>(1);
        trimmedElements.add(elements.get(0));
        this.elements = trimmedElements;
    }

    public void add(WrappedTreeSerializable wrappedElm)
    {
        add(wrappedElm.getTreeSerializable());
    }

    public void add(TreeSerializable elm)
    {
        assert hasCapacity(elm);

        if (elements.isEmpty())
        {
            sizeOfAddedElements += Short.BYTES; // need to add fixed overhead to size calculation for final elements ending offset
        }

        elements.add(new WrappedTreeSerializable(elm));
        sizeOfAddedElements += serializedLength(elm);
    }

    /**
     * Serializes the contents of this Node for use in a
     * {@link org.apache.cassandra.db.index.birch.BirchWriter} tree (see section 1.2 and 1.3).
     *
     * @param writer the instance of PageAlignedWriter to write this Node to
     * @throws IOException thrown if an IO error is encountered while serializing
     * @see BirchWriter
     */
    public void serialize(PageAlignedWriter writer) throws IOException
    {
        long pageFileOffsetStart = writer.getCurrentFilePosition();

        writer.writeShort(elements.size());

        int sizeInBytesOfOverflowField = bytesRequiredForHasOverflowField();

        byte[] overflowField = new byte[sizeInBytesOfOverflowField];

        int relativeOffsetsStart = Short.BYTES;
        int relativeValuesOffsetsStart = relativeOffsetsStart + ((elements.size() + 1) * Short.BYTES);
        int valueSize = elements.get(0).getTreeSerializable().serializedValueSize();
        int relativeStartOfHasOverflow = relativeValuesOffsetsStart + (elements.size() * valueSize);

        int currentKeyOffset = relativeStartOfHasOverflow + sizeInBytesOfOverflowField;
        for (int i = 0; i < elements.size(); i++)
        {
            writer.seek(pageFileOffsetStart + relativeOffsetsStart + (Short.BYTES * i));
            writer.writeShort(currentKeyOffset);
            writer.seek(pageFileOffsetStart + relativeValuesOffsetsStart + (valueSize * i));

            WrappedTreeSerializable currentElement = elements.get(i);
            currentElement.setSerializedOffset(pageFileOffsetStart);

            if (currentElement.getTreeSerializable() instanceof Node)
            {
                writer.writeLong(((Node) currentElement.getTreeSerializable()).getElementToPromote().getSerializedOffset());
            }
            else
            {
                currentElement.getTreeSerializable().serializeValue(writer);
            }

            writer.seek(pageFileOffsetStart + currentKeyOffset);

            ByteBuffer key = currentElement.getTreeSerializable().serializedKey(type);
            int keyStartingPosition = key.position();
            int totalKeyLength = key.limit() - keyStartingPosition;

            int keySize;
            if (totalKeyLength > maxKeyLength)
            {
                // key being added overflows :(, need to split the key into node page and overflow page
                writer.write(key, keyStartingPosition, maxKeyLength);

                byte[] overflowBuf = new byte[totalKeyLength - maxKeyLength];
                int keyOverflowStartingPosition = keyStartingPosition + maxKeyLength;
                ByteBufferUtil.arrayCopy(key, keyOverflowStartingPosition, overflowBuf, 0, overflowBuf.length);
                long overflowOffset = overflow.add(ByteBuffer.wrap(overflowBuf));
                writer.writeLong(overflowOffset);

                // the key size written into this node page is the max without overflow
                keySize = maxKeyLength + Long.BYTES;

                int overflowByteOffset = getByteOffsetInOverflowField(i);
                byte overflowByte = overflowField[overflowByteOffset];
                overflowByte |= 1 << (i % 8);
                overflowField[overflowByteOffset] = overflowByte;
            }
            else
            {
                // the key size written into this node page is the total key length, as it doesn't overflow
                keySize = totalKeyLength;
                writer.write(key);
            }

            // increment the relative offset in this node's page by the amount written (exclusive of overflow)
            currentKeyOffset += keySize;

            // check if we are on the last element, if so we need to encode the ending position into the offsets
            if (i + 1 == elements.size())
            {
                PageAlignedFileMark mark = writer.mark();
                writer.seek(pageFileOffsetStart + relativeOffsetsStart + (Short.BYTES * (i + 1)));
                writer.writeShort(currentKeyOffset);

                // encode the hasOverflow bit field
                writer.seek(pageFileOffsetStart + relativeStartOfHasOverflow);
                writer.write(overflowField, 0, overflowField.length);

                writer.reset(mark);
            }
        }

        writer.seek(pageFileOffsetStart + pageSize);
    }

    public WrappedTreeSerializable getElementToPromote()
    {
        return elements.get(0);
    }

    public int getNumberOfElements()
    {
        return elements.size();
    }

    private short serializedLength(TreeSerializable elm)
    {
        int keyLength = elm.serializedKeySize(type);
        boolean hasOverflow = keyLength > maxKeyLength;

        short length = (hasOverflow) ? maxKeyLength : (short) keyLength;
        length += Short.BYTES; // offset in page to key bytes
        if (elm instanceof Node)
        {
            length += Long.BYTES; // file offset to next pointer for this node
        }
        else
        {
            length += serializer.serializedValueSize();
        }

        if (hasOverflow)
            length += Long.BYTES;

        assert length >= 0; // protect against possible overflow of the short

        return length;
    }

    private int bytesRequiredForHasOverflowField()
    {
        return getByteOffsetInOverflowField(elements.size()) + 1;
    }

    private static int getByteOffsetInOverflowField(int elementIdx)
    {
        return Byte.BYTES * (int) (elementIdx / 8.0);
    }

    public int serializedKeySize(CType type)
    {
        return getElementToPromote().getTreeSerializable().serializedKeySize(type);
    }

    public ByteBuffer serializedKey(CType type)
    {
        return getElementToPromote().getTreeSerializable().serializedKey(type);
    }

    public int serializedValueSize()
    {
        return Long.BYTES;
    }

    public void serializeValue(PageAlignedWriter writer) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
