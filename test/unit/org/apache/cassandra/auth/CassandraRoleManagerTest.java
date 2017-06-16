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

package org.apache.cassandra.auth;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;

import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.StorageService;
import org.mindrot.jbcrypt.BCrypt;

import static org.junit.Assert.assertEquals;

public class CassandraRoleManagerTest
{

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.loadSchema();
        // We start StorageService because confirmFastRoleSetup confirms that CassandraRoleManager will
        // take a faster path once the cluster is already setup, which includes checking MessagingService
        // and issuing queries with QueryProcessor.process, which uses TokenMetadata
        StorageService.instance.initServer(0);
    }


    @Test
    public void confirmFastRoleSetup() throws Exception
    {

        CassandraRoleManager crm = new CassandraRoleManager();
        assertEquals(CassandraRoleManager.hasExistingRoles(), false);
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) " +
                                             "VALUES ('%s', true, true, '%s')",
                                             SchemaConstants.AUTH_KEYSPACE_NAME,
                                             AuthKeyspace.ROLES,
                                             CassandraRoleManager.DEFAULT_SUPERUSER_NAME,
                                             BCrypt.hashpw(CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD, BCrypt.gensalt(10))
                                                   .replace("'", "''"),
                               CassandraRoleManager.consistencyForRole(CassandraRoleManager.DEFAULT_SUPERUSER_NAME)));

        assertEquals(CassandraRoleManager.hasExistingRoles(), true);
        assertEquals(crm.isClusterReady(), false);

        crm.setup();

        // isClusterReady should toggle immediately, without waiting for the scheduled task
        assertEquals(CassandraRoleManager.hasExistingRoles(), true);
        assertEquals(crm.isClusterReady(), true);
    }

}