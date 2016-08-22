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

package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.index.birch.AlignedSegment;
import org.apache.cassandra.db.index.birch.PageAlignedReader;
import org.apache.cassandra.io.FSReadError;

public class PageAlignedAwareSegmentedFile extends SegmentedFile
{
    private static final Logger logger = LoggerFactory.getLogger(PageAlignedAwareSegmentedFile.class);

    private final String path;

    private PageAlignedAwareSegmentedFile(String path)
    {
        super(new Cleanup(path), path, new File(path).length());
        this.path = path;
    }

    private PageAlignedAwareSegmentedFile(PageAlignedAwareSegmentedFile copy)
    {
        super(copy);
        this.path = copy.path;
    }

    public PageAlignedAwareSegmentedFile sharedCopy()
    {
        return new PageAlignedAwareSegmentedFile(this);
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        private Cleanup(String path)
        {
            super(path);
        }

        public void tidy()
        {
        }
    }

    public static class Builder extends SegmentedFile.Builder
    {
        public Builder()
        {
            super();
        }

        public void addPotentialBoundary(long boundary)
        {

        }

        protected SegmentedFile complete(String path, long overrideLength, boolean isFinal)
        {

            return new PageAlignedAwareSegmentedFile(path);
        }
    }

    /**
     * Gets a new instance of a SegmentIterator implementation to iterate over segments.
     * The first segment to be returned will be the segment that contains the given position.
     * <p>
     * It is the callers responsibility to close each segment after use.
     *
     * @param position the position in the index of the first segment to start the iterator at
     * @return a SegmentIterator starting at the provided position to iterate segments
     *
     */
    @Override
    public SegmentIterator indexIterator(long position)
    {
        return new PageAlignedAwareSegmentedFile.PageAlignedSegmentIterator(position);
    }

    public FileDataInput getSegment(long position)
    {
        throw new UnsupportedOperationException();
    }

    private class PageAlignedSegmentIterator extends SegmentIterator
    {
        private final PageAlignedReader reader;
        private final PageAlignedReader.AlignedSegmentIterator segmentIterator;

        private PageAlignedSegmentIterator(long position)
        {
            try
            {
                PageAlignedReader reader = new PageAlignedReader(new File(path));
                this.reader = reader;
                this.segmentIterator = reader.getSegmentIterator(reader.findIdxForPosition(position));
            }
            catch (IOException e)
            {
                logger.error("Fatal exception while creating segment iterator for {}", path, e);
                throw new FSReadError(e, path);
            }
        }

        public boolean hasNext()
        {
            return segmentIterator.hasNext();
        }

        public FileDataInput next()
        {
            AlignedSegment next = segmentIterator.next();
            try
            {
                PageAlignedReader readerForSegment = PageAlignedReader.copy(reader);
                readerForSegment.setSegment(next.idx, 0);
                readerForSegment.seek(next.offset);
                return readerForSegment;
            }
            catch (IOException e)
            {
                throw new FSReadError(e, path);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception
        {
            reader.close();
        }
    }
}
