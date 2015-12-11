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

import org.apache.cassandra.exceptions.ConfigurationException;

import java.util.Set;


/*
 * CASSANDRA-7306: As clusters get larger and larger, allow operators to determine
 * precisely how dc-to-dc connections should be routed. This allows operators to
 * create pluggable topology providers to enable topologies such as hub-and-spoke
 * and daisy-chain / ring topologies
 *
 * Each NODE is given the opportunity to specify which DATACENTERS it will communicate
 * with for the purpose of both gossip and requests.
 */
public interface IDatacenterTopologyProvider {

    public Set<String> filteredDatacenters(Set<String> rawDatacenterSet);

    public boolean filtersDatacenters();

    public boolean isGossipableDatacenter(String dc) throws ConfigurationException;

    public void reloadDatacenterTopologyProvider() throws ConfigurationException;
}
