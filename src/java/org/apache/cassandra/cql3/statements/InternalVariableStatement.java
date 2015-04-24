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
package org.apache.cassandra.cql3.statements;

import java.util.List;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.SystemKeyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class InternalVariableStatement extends ParsedStatement implements CQLStatement
{
    // pseudo-virtual cf
    private static final String KS = SystemKeyspace.NAME;
    private static final String CF = "variables";

    @Override
    public Prepared prepare()
    {
        return new Prepared(this);
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public void checkAccess(ClientState state)
    {
        // TODO
    }

    public ResultMessage execute(QueryState state, QueryOptions options)
    throws RequestValidationException, RequestExecutionException
    {
        return execute(state.getClientState());
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        return null;
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        state.validateLogin();
    }

    public static class ShowVariableStatement extends InternalVariableStatement
    {
        private static final List<ColumnSpecification> metadata =
                ImmutableList.of(new ColumnSpecification(KS, CF, new ColumnIdentifier("variable", true), UTF8Type.instance),
                        new ColumnSpecification(KS, CF, new ColumnIdentifier("value", true), UTF8Type.instance));

        private String singleVariable = null;

        public ShowVariableStatement()
        {
            singleVariable = null;
        }

        public ShowVariableStatement(String singleVariableName)
        {
            singleVariable = singleVariableName.trim();
        }

        private ResultSet result = new ResultSet(metadata);

        private void addRow(String variableName, String variableValue)
        {
            if (singleVariable == null || variableName.equals(singleVariable) ) {
                result.addColumnValue(UTF8Type.instance.fromString(variableName));
                result.addColumnValue(UTF8Type.instance.fromString(variableValue));
            }
        }

        public ResultMessage execute(QueryState state, QueryOptions options)
                throws RequestValidationException, RequestExecutionException
        {
            return execute(state.getClientState());
        }

        public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
        {

            addRow("authenticator",                             DatabaseDescriptor.getAuthenticatorName());
            addRow("authorizer",                                DatabaseDescriptor.getAuthorizerName());
            addRow("auto_snapshot",                             String.valueOf(DatabaseDescriptor.isAutoSnapshot()));
            addRow("batch_size_fail_threshold_in_kb",           String.valueOf(DatabaseDescriptor.getBatchSizeFailThreshold() / 1024));
            addRow("batch_size_warn_threshold_in_kb",           String.valueOf(DatabaseDescriptor.getBatchSizeWarnThreshold() / 1024));
            addRow("batchlog_replay_throttle_in_kb",            String.valueOf(DatabaseDescriptor.getBatchlogReplayThrottleInKB()));
            addRow("cas_contention_timeout_in_ms",              String.valueOf(DatabaseDescriptor.getCasContentionTimeout()));
            addRow("cluster_name",                              String.valueOf(DatabaseDescriptor.getClusterName()));
            addRow("column_index_size_in_kb",                   String.valueOf(DatabaseDescriptor.getColumnIndexSize() / 1024));
            addRow("commit_failure_policy",                     String.valueOf(DatabaseDescriptor.getCommitFailurePolicyName()));
            addRow("commitlog_directory",                       DatabaseDescriptor.getCommitLogLocation());
            addRow("commitlog_segment_size_in_mb",              String.valueOf(DatabaseDescriptor.getCommitLogSegmentSize()));
            addRow("commitlog_sync",                            String.valueOf(DatabaseDescriptor.getCommitLogSync()));
            addRow("commitlog_sync_period_in_ms",               String.valueOf(DatabaseDescriptor.getCommitLogSyncPeriod()));
            addRow("compaction_throughput_mb_per_sec",          String.valueOf(DatabaseDescriptor.getCompactionThroughputMbPerSec()));
            addRow("concurrent_compactors",                     String.valueOf(DatabaseDescriptor.getConcurrentCompactors()));
            addRow("concurrent_counter_writes",                 String.valueOf(DatabaseDescriptor.getConcurrentCounterWriters()));
            addRow("concurrent_reads",                          String.valueOf(DatabaseDescriptor.getConcurrentReaders()));
            addRow("concurrent_writes",                         String.valueOf(DatabaseDescriptor.getConcurrentWriters()));
            addRow("counter_cache_save_period",                 String.valueOf(DatabaseDescriptor.getCounterCacheSavePeriod()));
            addRow("counter_cache_size_in_mb",                  String.valueOf(DatabaseDescriptor.getCounterCacheSizeInMB()));
            addRow("counter_write_request_timeout_in_ms",       String.valueOf(DatabaseDescriptor.getCounterWriteRpcTimeout()));
            addRow("cross_node_timeout",                        String.valueOf(DatabaseDescriptor.hasCrossNodeTimeout()));
            addRow("disk_failure_policy",                       DatabaseDescriptor.getDiskFailurePolicy().name());
            addRow("dynamic_snitch_badness_threshold",          String.valueOf(DatabaseDescriptor.getDynamicBadnessThreshold()));
            addRow("dynamic_snitch_reset_interval_in_ms",       String.valueOf(DatabaseDescriptor.getDynamicResetInterval()));
            addRow("dynamic_snitch_update_interval_in_ms",      String.valueOf(DatabaseDescriptor.getDynamicUpdateInterval()));
            addRow("endpoint_snitch",                           DatabaseDescriptor.getEndpointSnitchName());
            addRow("hinted_handoff_enabled",                    String.valueOf(DatabaseDescriptor.hintedHandoffEnabled()));
            addRow("hinted_handoff_throttle_in_kb",             String.valueOf(DatabaseDescriptor.getHintedHandoffThrottleInKB()));
            addRow("incremental_backups",                       String.valueOf(DatabaseDescriptor.isIncrementalBackupsEnabled()));
            addRow("index_summary_capacity_in_mb",              String.valueOf(DatabaseDescriptor.getIndexSummaryCapacityInMB()));
            addRow("index_summary_resize_interval_in_minutes",  String.valueOf(DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes()));
            addRow("initial_token",                             String.valueOf(DatabaseDescriptor.getInitialTokens()));
            addRow("inter_dc_stream_throughput_outbound_megabits_per_sec", String.valueOf(DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec()));
            addRow("inter_dc_tcp_nodelay",                      String.valueOf(DatabaseDescriptor.getInterDCTcpNoDelay()));
            addRow("internode_compression",                     String.valueOf(DatabaseDescriptor.internodeCompression().name()));
            addRow("key_cache_save_period",                     String.valueOf(DatabaseDescriptor.getKeyCacheSavePeriod()));
            addRow("key_cache_size_in_mb",                      String.valueOf(DatabaseDescriptor.getKeyCacheSizeInMB()));
            addRow("key_cache_keys_to_save",                    String.valueOf(DatabaseDescriptor.getKeyCacheKeysToSave()));
            addRow("listen_address",                            String.valueOf(DatabaseDescriptor.getListenAddress()));
            addRow("max_hint_window_in_ms",                     String.valueOf(DatabaseDescriptor.getMaxHintWindow()));
            addRow("max_hints_delivery_threads",                String.valueOf(DatabaseDescriptor.getMaxHintsThread()));
            addRow("memtable_allocation_type",                  DatabaseDescriptor.getMemtableAllocationTypeName());
            addRow("native_transport_port",                     String.valueOf(DatabaseDescriptor.getNativeTransportPort()));
            addRow("num_tokens",                                String.valueOf(DatabaseDescriptor.getNumTokens()));
            addRow("partitioner",                               DatabaseDescriptor.getPartitionerName());
            addRow("permissions_validity_in_ms",                String.valueOf(DatabaseDescriptor.getPermissionsValidity()));
            addRow("phi_convict_threshold",                     String.valueOf(DatabaseDescriptor.getPhiConvictThreshold()));
            addRow("range_request_timeout_in_ms",               String.valueOf(DatabaseDescriptor.getRangeRpcTimeout()));
            addRow("read_request_timeout_in_ms",                String.valueOf(DatabaseDescriptor.getReadRpcTimeout()));
            addRow("request_scheduler",                         DatabaseDescriptor.getRequestSchedulerName());
            addRow("request_timeout_in_ms",                     String.valueOf(DatabaseDescriptor.getRpcTimeout()));
            addRow("role_manager",                              DatabaseDescriptor.getRoleManagerName());
            addRow("roles_validity_in_ms",                      String.valueOf(DatabaseDescriptor.getRolesValidity()));
            addRow("row_cache_keys_to_save",                    String.valueOf(DatabaseDescriptor.getRowCacheKeysToSave()));
            addRow("row_cache_save_period",                     String.valueOf(DatabaseDescriptor.getRowCacheSavePeriod()));
            addRow("row_cache_size_in_mb",                      String.valueOf(DatabaseDescriptor.getRowCacheSizeInMB()));
            addRow("rpc_address",                               String.valueOf(DatabaseDescriptor.getRpcAddress()));
            addRow("rpc_keepalive",                             String.valueOf(DatabaseDescriptor.getRpcKeepAlive()));
            addRow("rpc_min_threads",                           String.valueOf(DatabaseDescriptor.getRpcMinThreads()));
            addRow("rpc_max_threads",                           String.valueOf(DatabaseDescriptor.getRpcMaxThreads()));
            addRow("rpc_port",                                  String.valueOf(DatabaseDescriptor.getRpcPort()));
            addRow("rpc_recv_buff_size_in_bytes",               String.valueOf(DatabaseDescriptor.getRpcRecvBufferSize()));
            addRow("rpc_send_buff_size_in_bytes",               String.valueOf(DatabaseDescriptor.getRpcSendBufferSize()));
            addRow("rpc_server_type",                           String.valueOf(DatabaseDescriptor.getRpcServerType()));
            addRow("seed_provider",                             DatabaseDescriptor.getSeedProviderName());
            addRow("seeds",                                     String.valueOf(DatabaseDescriptor.getSeeds()));
            addRow("server_encryption_options.internode_encryption", DatabaseDescriptor.getServerEncryptionOptions().internode_encryption.name());
            addRow("server_encryption_options.protocol",        DatabaseDescriptor.getServerEncryptionOptions().protocol);
            addRow("snapshot_before_compaction", String.valueOf(DatabaseDescriptor.isSnapshotBeforeCompaction()));
            addRow("ssl_storage_port",                          String.valueOf(DatabaseDescriptor.getSSLStoragePort()));
            addRow("sstable_preemptive_open_interval_in_mb",    String.valueOf(DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB()));
            addRow("start_native_transport",                    String.valueOf(DatabaseDescriptor.startNativeTransport()));
            addRow("start_rpc",                                 String.valueOf(DatabaseDescriptor.startRpc()));
            addRow("streaming_socket_timeout_in_ms",            String.valueOf(DatabaseDescriptor.getStreamingSocketTimeout()));
            addRow("stream_throughput_outbound_megabits_per_sec", String.valueOf(DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec()));
            addRow("storage_port",                              String.valueOf(DatabaseDescriptor.getStoragePort()));
            addRow("thrift_framed_transport_size_in_mb",        String.valueOf(DatabaseDescriptor.getThriftFramedTransportSize() / (1024 * 1024)));
            addRow("tombstone_failure_threshold",               String.valueOf(DatabaseDescriptor.getTombstoneFailureThreshold()));
            addRow("tombstone_warn_threshold",                  String.valueOf(DatabaseDescriptor.getTombstoneWarnThreshold()));
            addRow("tracetype_query_ttl",                       String.valueOf(DatabaseDescriptor.getTracetypeQueryTTL()));
            addRow("tracetype_repair_ttl",                      String.valueOf(DatabaseDescriptor.getTracetypeRepairTTL()));
            addRow("trickle_fsync",                             String.valueOf(DatabaseDescriptor.getTrickleFsync()));
            addRow("trickle_fsync_interval_in_kb",              String.valueOf(DatabaseDescriptor.getTrickleFsyncIntervalInKb()));
            addRow("truncate_request_timeout_in_ms",            String.valueOf(DatabaseDescriptor.getTruncateRpcTimeout()));
            addRow("write_request_timeout_in_ms",               String.valueOf(DatabaseDescriptor.getWriteRpcTimeout()));

            if (result.rows.size() == 0)
                throw new InvalidRequestException(String.format("Unable to display variable %s ", singleVariable));

            return new ResultMessage.Rows(result);

        }

    }

    public static class SetVariableStatement extends InternalVariableStatement
    {

        private String setVariableName = null;
        private String setVariableValue = null;
        private static final List<ColumnSpecification> metadata =
                ImmutableList.of(new ColumnSpecification(KS, CF, new ColumnIdentifier("variable", true), UTF8Type.instance),
                        new ColumnSpecification(KS, CF, new ColumnIdentifier("result", true), BooleanType.instance));


        private static final Logger logger = LoggerFactory.getLogger(SetVariableStatement.class);

        public SetVariableStatement(String variableName, String variableValue)
        {
            logger.info(String.format("Changing system variable %s to %s", variableName, variableValue));
            setVariableName = variableName;
            setVariableValue = variableValue;
        }

        public ResultMessage execute(QueryState state, QueryOptions options)
                throws RequestValidationException, RequestExecutionException
        {
            return execute(state.getClientState());
        }

        public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
        {
            ResultSet result = new ResultSet(metadata);
            switch (setVariableName) {
                case "auto_snapshot":
                    DatabaseDescriptor.setAutoSnapshot(Boolean.valueOf(setVariableValue));
                    break;
                case "batch_size_fail_threshold_in_kb":
                    DatabaseDescriptor.setBatchSizeFailThresholdInKB(Integer.valueOf(setVariableValue));
                    break;
                case "batch_size_warn_threshold_in_kb":
                    DatabaseDescriptor.setBatchSizeWarnThresholdInKB(Integer.valueOf(setVariableValue));
                    break;
                case "batchlog_replay_throttle_in_kb":
                    DatabaseDescriptor.setBatchlogReplayThrottleInKB(Integer.valueOf(setVariableValue));
                    break;
                case "cas_contention_timeout_in_ms":
                    DatabaseDescriptor.setCasContentionTimeout(Long.valueOf(setVariableValue));
                    break;
                case "column_index_size_in_kb":
                    DatabaseDescriptor.setColumnIndexSize(Integer.valueOf(setVariableValue));
                    break;
                case "commit_failure_policy":
                    Config.CommitFailurePolicy commitFailurePolicy;
                    switch(setVariableValue)
                    {
                        case "stop":
                            commitFailurePolicy = Config.CommitFailurePolicy.stop;
                            break;
                        case "stop_commit":
                            commitFailurePolicy = Config.CommitFailurePolicy.stop_commit;
                            break;
                        case "ignore":
                            commitFailurePolicy = Config.CommitFailurePolicy.ignore;
                            break;
                        case "die":
                            commitFailurePolicy = Config.CommitFailurePolicy.die;
                            break;
                        default:
                            logger.warn(String.format("Unable to change variable %s to %s - invalid commit failure policy", setVariableName, setVariableValue));
                            throw new InvalidRequestException(String.format("Unable to change variable %s to %s - invalid commit failure policy", setVariableName, setVariableValue));
                    }
                    DatabaseDescriptor.setCommitFailurePolicy(commitFailurePolicy);
                    break;
                case "commitlog_sync_period_in_ms":
                    DatabaseDescriptor.setCommitLogSyncPeriod(Integer.valueOf(setVariableValue));
                    break;
                case "compaction_throughput_mb_per_sec":
                    DatabaseDescriptor.setCompactionThroughputMbPerSec(Integer.valueOf(setVariableValue));
                    break;
                case "counter_cache_save_period":
                    DatabaseDescriptor.setCounterCacheSavePeriod(Integer.valueOf(setVariableValue));
                    break;
                case "counter_cache_size_in_mb":
                    DatabaseDescriptor.setCounterCacheSizeInMB(Integer.valueOf(setVariableValue));
                    break;
                case "counter_write_request_timeout_in_ms":
                    DatabaseDescriptor.setCounterWriteRpcTimeout(Long.valueOf(setVariableValue));
                    break;
                case "disk_failure_policy":
                    Config.DiskFailurePolicy diskFailurePolicy;
                    switch(setVariableValue)
                    {
                        case "best_effort":
                            diskFailurePolicy = Config.DiskFailurePolicy.best_effort;
                            break;
                        case "stop":
                            diskFailurePolicy = Config.DiskFailurePolicy.stop;
                            break;
                        case "ignore":
                            diskFailurePolicy = Config.DiskFailurePolicy.ignore;
                            break;
                        case "die":
                            diskFailurePolicy = Config.DiskFailurePolicy.die;
                            break;
                        case "stop_paranoid":
                            diskFailurePolicy = Config.DiskFailurePolicy.stop_paranoid;
                            break;
                        default:
                            logger.warn(String.format("Unable to change variable %s to %s - invalid disk failure policy", setVariableName, setVariableValue));
                            throw new InvalidRequestException(String.format("Unable to change variable %s to %s - invalid disk failure policy", setVariableName, setVariableValue));
                    }
                    DatabaseDescriptor.setDiskFailurePolicy(diskFailurePolicy);
                    break;
                case "dynamic_snitch_badness_threshold":
                    DatabaseDescriptor.setDynamicBadnessThreshold(Double.valueOf(setVariableValue));
                    break;
                case "dynamic_snitch_reset_interval_in_ms":
                    DatabaseDescriptor.setDynamicResetInterval(Integer.valueOf(setVariableValue));
                    break;
                case "dynamic_snitch_update_interval_in_ms":
                    DatabaseDescriptor.setDynamicUpdateInterval(Integer.valueOf(setVariableValue));
                    break;
                case "hinted_handoff_throttle_in_kb":
                    DatabaseDescriptor.setHintedHandoffThrottleInKB(Integer.valueOf(setVariableValue));
                    break;
                case "incremental_backups":
                    DatabaseDescriptor.setIncrementalBackupsEnabled(Boolean.valueOf(setVariableValue));
                    break;
                case "index_summary_capacity_in_mb":
                    DatabaseDescriptor.setIndexSummaryCapacityInMB(Long.valueOf(setVariableValue));
                    break;
                case "index_summary_resize_interval_in_minutes":
                    DatabaseDescriptor.setIndexSummaryResizeIntervalInMinutes(Integer.valueOf(setVariableValue));
                    break;
                case "key_cache_save_period":
                    DatabaseDescriptor.setKeyCacheSavePeriod(Integer.valueOf(setVariableValue));
                    break;
                case "key_cache_keys_to_save":
                    DatabaseDescriptor.setKeyCacheKeysToSave(Integer.valueOf(setVariableValue));
                    break;
                case "max_hint_window_in_ms":
                    DatabaseDescriptor.setMaxHintWindow(Integer.valueOf(setVariableValue));
                    break;
                case "permissions_validity_in_ms":
                    DatabaseDescriptor.setPermissionsValidity(Integer.valueOf(setVariableValue));
                    break;
                case "phi_convict_threshold":
                    DatabaseDescriptor.setPhiConvictThreshold(Double.valueOf(setVariableValue));
                    break;
                case "read_request_timeout_in_ms":
                    DatabaseDescriptor.setReadRpcTimeout(Long.valueOf(setVariableValue));
                    break;
                case "request_timeout_in_ms":
                    DatabaseDescriptor.setRpcTimeout(Long.valueOf(setVariableValue));
                    break;
                case "roles_validity_in_ms":
                    DatabaseDescriptor.setRolesValidity(Integer.valueOf(setVariableValue));
                    break;
                case "row_cache_save_period":
                    DatabaseDescriptor.setRowCacheSavePeriod(Integer.valueOf(setVariableValue));
                    break;
                case "row_cache_keys_to_save":
                    DatabaseDescriptor.setRowCacheKeysToSave(Integer.valueOf(setVariableValue));
                    break;
                case "sstable_preemptive_open_interval_in_mb":
                    DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMB(Integer.valueOf(setVariableValue));
                    break;
                case "tombstone_failure_threshold":
                    DatabaseDescriptor.setTombstoneFailureThreshold(Integer.valueOf(setVariableValue));
                    break;
                case "tombstone_warn_threshold":
                    DatabaseDescriptor.setTombstoneWarnThreshold(Integer.valueOf(setVariableValue));
                    break;
                case "tracetype_query_ttl":
                    DatabaseDescriptor.setTracetypeQueryTTL(Integer.valueOf(setVariableValue));
                    break;
                case "tracetype_repair_ttl":
                    DatabaseDescriptor.setTracetypeRepairTTL(Integer.valueOf(setVariableValue));
                    break;
                case "truncate_request_timeout_in_ms":
                    DatabaseDescriptor.setTruncateRpcTimeout(Long.valueOf(setVariableValue));
                    break;
                case "write_request_timeout_in_ms":
                    DatabaseDescriptor.setWriteRpcTimeout(Long.valueOf(setVariableValue));
                    break;

                default:
                    logger.warn(String.format("Unable to change variable %s to %s - not in list of settable variables", setVariableName, setVariableValue));
                    throw new InvalidRequestException(String.format("Unable to change variable %s to %s - not in list of settable variables", setVariableName, setVariableValue));
                }
            result.addColumnValue(UTF8Type.instance.fromString(setVariableName));
            result.addColumnValue(BooleanType.instance.decompose(Boolean.TRUE));
            return new ResultMessage.Rows(result);
        }
    }
}

