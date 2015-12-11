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

package org.apache.cassandra.locator;

import com.google.common.net.InetAddresses;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IDatacenterTopologyProvider;
import org.apache.cassandra.locator.MeshDatacenterTopologyProvider;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link org.apache.cassandra.locator.MeshDatacenterTopologyProvider}
 * */
public class MeshDatacenterTopologyProviderTest
{

    @Test
    public void testBaseConfiguration() throws Exception
    {
        IDatacenterTopologyProvider dcTopologyProvider = DatabaseDescriptor.getDatacenterTopologyProvider();
        assert(dcTopologyProvider instanceof MeshDatacenterTopologyProvider);
        assertEquals(dcTopologyProvider.filtersDatacenters(), false);
        assertEquals(dcTopologyProvider.isGossipableDatacenter("datacenter1"), true);
        assertEquals(dcTopologyProvider.isGossipableDatacenter("nonexistent"), true);
    }
}
