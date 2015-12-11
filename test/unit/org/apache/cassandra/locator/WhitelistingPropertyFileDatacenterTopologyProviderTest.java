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

import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link org.apache.cassandra.locator.WhitelistingPropertyFileDatacenterTopologyProvider}
 * */
public class WhitelistingPropertyFileDatacenterTopologyProviderTest
{

    @Test
    public void testBaseConfiguration() throws Exception
    {
        IDatacenterTopologyProvider datacenterTopologyProvider = new WhitelistingPropertyFileDatacenterTopologyProvider();
        DatabaseDescriptor.setDatacenterTopologyProvider(datacenterTopologyProvider);

        Set<String> unfiltered = new TreeSet<>();
        Set<String> filtered = new TreeSet<>();
        unfiltered.add("datacenter1");
        unfiltered.add("datacenter2");
        unfiltered.add("datacenter3");
        filtered.add("datacenter1");
        filtered.add("datacenter2");

        IDatacenterTopologyProvider dcTopologyProvider = DatabaseDescriptor.getDatacenterTopologyProvider();
        assert(dcTopologyProvider instanceof WhitelistingPropertyFileDatacenterTopologyProvider);
        assertEquals(dcTopologyProvider.filtersDatacenters(), true);
        assertEquals(dcTopologyProvider.isGossipableDatacenter("datacenter1"), true);
        assertEquals(dcTopologyProvider.isGossipableDatacenter("datacenter2"), true);
        assertEquals(dcTopologyProvider.isGossipableDatacenter("datacenter3"), false);
        assertEquals(dcTopologyProvider.filteredDatacenters(unfiltered), filtered);
    }
}
