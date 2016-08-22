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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class AlignedSegment
{
    public final int idx;
    public final long offset;
    public final long length;
    public final long alignedLength;
    private final List<AlignedSubSegment> subSegments;

    public AlignedSegment(int idx, long offset, long length, long alignedLength)
    {
        this(idx, offset, length, alignedLength, new ArrayList<AlignedSubSegment>());
    }

    public AlignedSegment(int idx, long offset, long length, long alignedLength, List<AlignedSubSegment> subSegments)
    {
        this.idx = idx;
        this.offset = offset;
        this.length = length;
        this.alignedLength = alignedLength;
        this.subSegments = subSegments;
    }

    public void addSubSegment(AlignedSubSegment subSegment)
    {
        subSegments.add(subSegment);
    }

    public List<AlignedSubSegment> getSubSegments()
    {
        return subSegments;
    }

    public AlignedSubSegment getLastSubSegment()
    {
        return subSegments.get(subSegments.size() - 1);
    }

    public boolean isPositionInSegment(long pos)
    {
        return pos >= offset && pos <= offset + length;
    }

    @Override
    public String toString()
    {
        Joiner joiner = Joiner.on(" | ");
        String joinedSubSegments = joiner.join(subSegments);
        return String.format("[Segment %d] offset: %d length: %d alignedLength: %d " +
                             "numSubSegments: %d {%s}", idx, offset, length, alignedLength,
                             subSegments.size(), joinedSubSegments);
    }
}
