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

import com.google.common.base.Objects;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/*
 * CASSANDRA-7306: As clusters get larger and larger, allow operators to determine
 * precisely how dc-to-dc connections should be routed. This allows operators to
 * create pluggable topology providers to enable topologies such as hub-and-spoke
 * and daisy-chain / ring topologies
 *
 * Each NODE is given the opportunity to specify which DATACENTERS it will communicate
 * with for the purpose of both gossip and requests.
 */
public class BlacklistingPropertyFileDatacenterTopologyProvider implements IDatacenterTopologyProvider {

    private static final Logger logger = LoggerFactory.getLogger(BlacklistingPropertyFileDatacenterTopologyProvider.class);

    private static final int REFRESH_PERIOD_IN_SECONDS = 60;

    private Set<String> excludeDcs = new TreeSet<>();
    private Set<String> dcsWithHHDisabled = DatabaseDescriptor.hintedHandoffDisabledDCs();

    private int lastHashCode = -1;

    public BlacklistingPropertyFileDatacenterTopologyProvider()
    {
        logger.debug("Initializing BlacklistingPropertyFileDatacenterTopologyProvider");
        reloadConfiguration();
        try
        {
            FBUtilities.resourceToFile(DatacenterTopologyProperties.DATACENTER_TOPOLOGY_PROPERTY_FILE);
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws ConfigurationException
                {
                    reloadDatacenterTopologyProvider();
                }
            };
            ResourceWatcher.watch(DatacenterTopologyProperties.DATACENTER_TOPOLOGY_PROPERTY_FILE, runnable, REFRESH_PERIOD_IN_SECONDS * 1000);
        }
        catch (ConfigurationException ex)
        {
            logger.error("{} found, but does not look like a plain file. Will not watch it for changes", DatacenterTopologyProperties.DATACENTER_TOPOLOGY_PROPERTY_FILE);
        }
    }

    /*
     *
     */
    public Set<String> filteredDatacenters(Set<String> rawDatacenterSet)
    {
        Set<String> filteredDatacenters = new TreeSet<>(rawDatacenterSet);
        for(String dc : rawDatacenterSet)
            if(!isGossipableDatacenter(dc))
            {
                filteredDatacenters.remove(dc);
            }

        return filteredDatacenters;
    }

    public boolean isGossipableDatacenter(String dcName) throws ConfigurationException
    {
        if(excludeDcs != null)
        {
            if(!excludeDcs.contains(dcName.trim().toLowerCase()))
                return false;
            else
                return true;
        }
        else
            throw new ConfigurationException("Invalid state: BlacklistingPropertyFileDatacenterTopologyProvider improperly configured");

    }

    /*
     * Reload the config from disk
     */
    public void reloadDatacenterTopologyProvider() throws ConfigurationException
    {
        logger.debug("BlacklistingPropertyFileDatacenterTopologyProvider.reloadDatacenterTopologyProvider()");
        reloadConfiguration();
        for(String dc : excludeDcs)
        {
            if(!dcsWithHHDisabled.contains(dc))
            {
                logger.info("Disabling communication with dc " + dc + ", explicitly disabling hints");
                DatabaseDescriptor.disableHintsForDC(dc);
                excludeDcs.add(dc);
            }
        }
        if(lastHashCode != hashCode())
        {
            lastHashCode = hashCode();
            // For each KS, the replication strategy may need to be reinstantiated in order to take advantage of
            // changes in the list of datacenters. TODO: Find the right way to do this safely.
            for(String ksName : Schema.instance.getNonSystemKeyspaces())
            {
                Keyspace k = Keyspace.open(ksName);
                if(k.getReplicationStrategy() instanceof NetworkTopologyStrategy )
                {
                    logger.info("Resetting replication strategy of {}", k.getName());
                    k.resetReplicationStrategy();
                }
            }
        }
        return;
    }


    /*
     * Reload the configuration file from disk
     */
    private void reloadConfiguration() throws ConfigurationException
    {
        final DatacenterTopologyProperties properties = new DatacenterTopologyProperties();
        final String thisDatacenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress()).toLowerCase();
        final String invalidFilterMessage = "BlacklistingPropertyFileDatacenterTopologyProvider is set to filter local DC " + thisDatacenter + ", this is an invalid configuration";

        String excludeDcsRaw = properties.get("exclude_datacenters", null);
        if(excludeDcsRaw == null)
            throw new ConfigurationException("Invalid state: exclude_datacenters must be set in: " + DatacenterTopologyProperties.DATACENTER_TOPOLOGY_PROPERTY_FILE);

        this.excludeDcs = new TreeSet<>();
        excludeDcsRaw = excludeDcsRaw.trim();
        for (String dc : excludeDcsRaw.split(","))
            if (!this.excludeDcs.contains(dc))
            {
                if(dc.trim().toLowerCase().equals(thisDatacenter))
                    throw new ConfigurationException(invalidFilterMessage);
                logger.debug("Loading " + DatacenterTopologyProperties.DATACENTER_TOPOLOGY_PROPERTY_FILE + ", adding " + dc + " to set of invalid datacenters");
                this.excludeDcs.add(dc.trim().toLowerCase());
            }
    }

    public int hashCode()
    {
        return Objects.hashCode(excludeDcs);
    }

    static class DatacenterTopologyProperties
    {
        private static final Logger logger = LoggerFactory.getLogger(DatacenterTopologyProperties.class);
        public static final String DATACENTER_TOPOLOGY_PROPERTY_FILE = "cassandra-dctopology.properties";

        private Properties properties;

        public DatacenterTopologyProperties()
        {
            properties = new Properties();
            InputStream stream = null;
            String configURL = System.getProperty(DATACENTER_TOPOLOGY_PROPERTY_FILE);
            try
            {
                URL url;
                if (configURL == null)
                    url = SnitchProperties.class.getClassLoader().getResource(DATACENTER_TOPOLOGY_PROPERTY_FILE);
                else
                    url = new URL(configURL);

                stream = url.openStream(); // catch block handles potential NPE
                properties.load(stream);
            }
            catch (Exception e)
            {
                // do not throw exception here, just consider this an incomplete or an empty property file.
                logger.debug("Unable to read {}", ((configURL != null) ? configURL : DATACENTER_TOPOLOGY_PROPERTY_FILE));
            }
            finally
            {
                FileUtils.closeQuietly(stream);
            }
        }

        /**
         * Get a snitch property value or return defaultValue if not defined.
         */
        public String get(String propertyName, String defaultValue)
        {
            return properties.getProperty(propertyName, defaultValue);
        }
    }
}
