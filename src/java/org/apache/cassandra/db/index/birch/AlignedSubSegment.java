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

public class AlignedSubSegment
{
    public final short idx;
    public final long offset;
    public final long length;
    public final long alignedLength;
    public final boolean isPageAligned;

    public AlignedSubSegment(short idx, long offset, long length, long alignedLength, boolean isPageAligned)
    {
        this.idx = idx;
        this.offset = offset;
        this.length = length;
        this.alignedLength = alignedLength;
        this.isPageAligned = isPageAligned;
    }

    public boolean isPageAligned()
    {
        return isPageAligned;
    }

    public long getEndOffset()
    {
        return offset + length;
    }

    public long getAlignedEndOffset()
    {
        return offset + alignedLength;
    }

    public boolean isPositionInSubSegment(long pos)
    {
        return pos >= offset && pos <= getEndOffset();
    }

    @Override
    public String toString()
    {
        return String.format("idx: %d offset: %d length: %d alignedLength: %d isPageAligned: %b",
                             idx, offset, length, alignedLength, isPageAligned);
    }
}
