/**
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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * CASSANDRA-7306: As clusters get larger and larger, allow operators to determine
 * precisely how dc-to-dc connections should be routed. This allows operators to
 * create pluggable topology providers to enable topologies such as hub-and-spoke
 * and daisy-chain / ring topologies
 *
 * Each NODE is given the opportunity to specify which DATACENTERS it will communicate
 * with for the purpose of both gossip and requests.
 */
public class MeshDatacenterTopologyProvider implements IDatacenterTopologyProvider {

    private static final Logger logger = LoggerFactory.getLogger(MeshDatacenterTopologyProvider.class);

    public MeshDatacenterTopologyProvider()
    {
        logger.trace("Initializing MeshDatacenterTopologyProvider");
    }
    /*
     * In a full mesh, no filtering is performed
     */
    public Set<String> filteredDatacenters(Set<String> rawDatacenterSet)
    {
        return rawDatacenterSet;
    }

    /*
     * In a full mesh, no filtering is performed
     */
    public boolean filtersDatacenters()
    {
        return false;
    }

    /*
     * In a full mesh, no filtering is performed
     */
    public boolean isGossipableDatacenter(String dcName) throws ConfigurationException
    {
        return true;
    }

    /*
     * In a full mesh, no filtering is performed, so reload is a no-op
     */
    public void reloadDatacenterTopologyProvider() throws ConfigurationException
    {
        logger.trace("MeshDatacenterTopologyProvider.reloadDatacenterTopologyProvider()");
        return;
    }
}
