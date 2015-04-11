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
package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.resolvers.CellResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.resolvers.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;
import static org.apache.cassandra.utils.FBUtilities.json;
import static org.apache.cassandra.utils.FBUtilities.serialize;

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
public class LegacySchemaTables
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySchemaTables.class);

    public static final String KEYSPACES = "schema_keyspaces";
    public static final String COLUMNFAMILIES = "schema_columnfamilies";
    public static final String COLUMNS = "schema_columns";
    public static final String TRIGGERS = "schema_triggers";
    public static final String USERTYPES = "schema_usertypes";
    public static final String FUNCTIONS = "schema_functions";
    public static final String AGGREGATES = "schema_aggregates";

    public static final List<String> ALL = Arrays.asList(KEYSPACES, COLUMNFAMILIES, COLUMNS, TRIGGERS, USERTYPES, FUNCTIONS, AGGREGATES);

    private static final CFMetaData Keyspaces =
        compile(KEYSPACES,
                "keyspace definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "durable_writes boolean,"
                + "strategy_class text,"
                + "strategy_options text,"
                + "PRIMARY KEY ((keyspace_name))) "
                + "WITH COMPACT STORAGE");

    private static final CFMetaData Columnfamilies =
        compile(COLUMNFAMILIES,
                "table definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "bloom_filter_fp_chance double,"
                + "caching text,"
                + "cf_id uuid," // post-2.1 UUID cfid
                + "comment text,"
                + "compaction_strategy_class text,"
                + "compaction_strategy_options text,"
                + "comparator text,"
                + "compression_parameters text,"
                + "default_time_to_live int,"
                + "default_validator text,"
                + "dropped_columns map<text, bigint>,"
                + "dropped_columns_types map<text, text>,"
                + "gc_grace_seconds int,"
                + "is_dense boolean,"
                + "key_validator text,"
                + "local_read_repair_chance double,"
                + "max_compaction_threshold int,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_compaction_threshold int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "subcomparator text,"
                + "type text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name))");

    private static final CFMetaData Columns =
        compile(COLUMNS,
                "column definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "column_name text,"
                + "component_index int,"
                + "index_name text,"
                + "index_options text,"
                + "index_type text,"
                + "type text,"
                + "validator text,"
                + "column_resolver text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, column_name))");

    private static final CFMetaData Triggers =
        compile(TRIGGERS,
                "trigger definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "trigger_name text,"
                + "trigger_options map<text, text>,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, trigger_name))");

    private static final CFMetaData Usertypes =
        compile(USERTYPES,
                "user defined type definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "type_name text,"
                + "field_names list<text>,"
                + "field_types list<text>,"
                + "field_resolvers list<text>,"
                + "PRIMARY KEY ((keyspace_name), type_name))");

    private static final CFMetaData Functions =
        compile(FUNCTIONS,
                "user defined function definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "function_name text,"
                + "signature frozen<list<text>>,"
                + "argument_names list<text>,"
                + "argument_types list<text>,"
                + "body text,"
                + "is_deterministic boolean,"
                + "language text,"
                + "return_type text,"
                + "PRIMARY KEY ((keyspace_name), function_name, signature))");

    private static final CFMetaData Aggregates =
        compile(AGGREGATES,
                "user defined aggregate definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "aggregate_name text,"
                + "signature frozen<list<text>>,"
                + "argument_types list<text>,"
                + "final_func text,"
                + "initcond blob,"
                + "return_type text,"
                + "state_func text,"
                + "state_type text,"
                + "PRIMARY KEY ((keyspace_name), aggregate_name, signature))");

    public static final List<CFMetaData> All = Arrays.asList(Keyspaces, Columnfamilies, Columns, Triggers, Usertypes, Functions, Aggregates);

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SystemKeyspace.NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
    }

    /** add entries to system.schema_* for the hardcoded system definitions */
    public static void saveSystemKeyspaceSchema()
    {
        KSMetaData keyspace = Schema.instance.getKSMetaData(SystemKeyspace.NAME);
        // delete old, possibly obsolete entries in schema tables
        for (String table : ALL)
            executeOnceInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = ?", table), keyspace.name);
        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        makeCreateKeyspaceMutation(keyspace, FBUtilities.timestampMicros() + 1).apply();
    }

    public static Collection<KSMetaData> readSchemaFromSystemTables()
    {
        try (DataIterator serializedSchema = getSchemaPartitionsForTable(KEYSPACES))
        {
            List<KSMetaData> keyspaces = new ArrayList<>();

            while (serializedSchema.hasNext())
            {
                try (RowIterator partition = serializedSchema.next())
                {
                    if (isSystemKeyspaceSchemaPartition(partition))
                        continue;

                    DecoratedKey key = partition.partitionKey();
                    try (RowIterator tables = readSchemaPartitionForKeyspace(COLUMNFAMILIES, key);
                         RowIterator types = readSchemaPartitionForKeyspace(USERTYPES, key);
                         RowIterator functions = readSchemaPartitionForKeyspace(FUNCTIONS, key);
                         RowIterator aggregates = readSchemaPartitionForKeyspace(AGGREGATES, key))
                    {
                        keyspaces.add(createKeyspaceFromSchemaPartitions(partition, tables, types));

                        // Will be moved away in #6717
                        for (UDFunction function : createFunctionsFromFunctionsPartition(functions))
                            org.apache.cassandra.cql3.functions.Functions.addFunction(function);

                        // Will be moved away in #6717
                        for (UDAggregate aggregate : createAggregatesFromAggregatesPartition(aggregates))
                            org.apache.cassandra.cql3.functions.Functions.addFunction(aggregate);
                    }
                }
            }
            return keyspaces;
        }
    }

    public static void truncateSchemaTables()
    {
        for (String table : ALL)
            getSchemaCFS(table).truncateBlocking();
    }

    private static void flushSchemaTables()
    {
        for (String table : ALL)
            SystemKeyspace.forceBlockingFlush(table);
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public static UUID calculateSchemaDigest()
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

        for (String table : ALL)
        {
            try (DataIterator schema = getSchemaPartitionsForTable(table))
            {
                while (schema.hasNext())
                {
                    try (RowIterator partition = schema.next())
                    {
                        if (!isSystemKeyspaceSchemaPartition(partition))
                            RowIterators.digest(partition, digest);
                    }
                }
            }
        }
        return UUID.nameUUIDFromBytes(digest.digest());
    }

    /**
     * @param schemaTableName The name of the table responsible for part of the schema
     * @return CFS responsible to hold low-level serialized schema
     */
    private static ColumnFamilyStore getSchemaCFS(String schemaTableName)
    {
        return Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(schemaTableName);
    }

    /**
     * @param schemaTableName The name of the table responsible for part of the schema.
     * @return low-level schema representation
     */
    private static DataIterator getSchemaPartitionsForTable(String schemaTableName)
    {
        ColumnFamilyStore cfs = getSchemaCFS(schemaTableName);
        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(cfs.metadata, FBUtilities.nowInSeconds());
        return PartitionIterators.asDataIterator(cmd.executeLocally(cfs));
    }

    public static Collection<Mutation> convertSchemaToMutations()
    {
        Map<DecoratedKey, Mutation> mutationMap = new HashMap<>();

        for (String table : ALL)
            convertSchemaToMutations(mutationMap, table);

        return mutationMap.values();
    }

    private static void convertSchemaToMutations(Map<DecoratedKey, Mutation> mutationMap, String schemaTableName)
    {
        try (DataIterator iter = getSchemaPartitionsForTable(schemaTableName))
        {
            while (iter.hasNext())
            {
                try (RowIterator partition = iter.next())
                {
                    if (isSystemKeyspaceSchemaPartition(partition))
                        continue;

                    DecoratedKey key = partition.partitionKey();
                    Mutation mutation = mutationMap.get(key);
                    if (mutation == null)
                    {
                        mutation = new Mutation(SystemKeyspace.NAME, key);
                        mutationMap.put(key, mutation);
                    }

                    mutation.add(RowIterators.toUpdate(partition));
                }
            }
        }
    }

    private static Map<DecoratedKey, ReadPartition> readSchemaForKeyspaces(String schemaTableName, Set<String> keyspaceNames)
    {
        Map<DecoratedKey, ReadPartition> schema = new HashMap<>();

        for (String keyspaceName : keyspaceNames)
        {
            // We don't to return the RowIterator directly because we should guarantee that this iterator
            // will be closed, and putting it in a Map make that harder/more awkward.
            try (RowIterator iter = readSchemaPartitionForKeyspace(schemaTableName, keyspaceName))
            {
                if (!RowIterators.isEmpty(iter))
                    schema.put(iter.partitionKey(), ReadPartition.create(iter));
            }
        }

        return schema;
    }

    private static ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }

    private static DecoratedKey getSchemaKSDecoratedKey(String ksName)
    {
        return StorageService.getPartitioner().decorateKey(getSchemaKSKey(ksName));
    }

    private static RowIterator readSchemaPartitionForKeyspace(String schemaTableName, String keyspaceName)
    {
        return readSchemaPartitionForKeyspace(schemaTableName, getSchemaKSDecoratedKey(keyspaceName));
    }

    private static RowIterator readSchemaPartitionForKeyspace(String schemaTableName, DecoratedKey keyspaceKey)
    {
        ColumnFamilyStore schemaCFS = getSchemaCFS(schemaTableName);
        return AtomIterators.asRowIterator(SinglePartitionReadCommand.fullPartitionRead(schemaCFS.metadata, FBUtilities.nowInSeconds(), keyspaceKey)
                                                                     .queryMemtableAndDisk(schemaCFS));
    }

    private static RowIterator readSchemaPartitionForTable(String schemaTableName, String keyspaceName, String tableName)
    {
        ColumnFamilyStore store = getSchemaCFS(schemaTableName);

        ClusteringComparator comparator = store.metadata.comparator;
        Slices slices = Slices.with(comparator, Slice.make(comparator, tableName));
        return AtomIterators.asRowIterator(SinglePartitionSliceCommand.create(store.metadata, FBUtilities.nowInSeconds(), getSchemaKSDecoratedKey(keyspaceName), slices)
                                                                      .queryMemtableAndDisk(store));
    }

    private static boolean isSystemKeyspaceSchemaPartition(RowIterator partition)
    {
        return getSchemaKSKey(SystemKeyspace.NAME).equals(partition.partitionKey().getKey());
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static synchronized void mergeSchema(Collection<Mutation> mutations) throws ConfigurationException, IOException
    {
        mergeSchema(mutations, true);
        Schema.instance.updateVersionAndAnnounce();
    }

    public static synchronized void mergeSchema(Collection<Mutation> mutations, boolean doFlush) throws IOException
    {
        // compare before/after schemas of the affected keyspaces only
        Set<String> keyspaces = new HashSet<>(mutations.size());
        for (Mutation mutation : mutations)
            keyspaces.add(ByteBufferUtil.string(mutation.key().getKey()));

        // current state of the schema
        Map<DecoratedKey, ReadPartition> oldKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, ReadPartition> oldColumnFamilies = readSchemaForKeyspaces(COLUMNFAMILIES, keyspaces);
        Map<DecoratedKey, ReadPartition> oldTypes = readSchemaForKeyspaces(USERTYPES, keyspaces);
        Map<DecoratedKey, ReadPartition> oldFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, ReadPartition> oldAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        for (Mutation mutation : mutations)
            mutation.apply();

        if (doFlush)
            flushSchemaTables();

        // with new data applied
        Map<DecoratedKey, ReadPartition> newKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, ReadPartition> newColumnFamilies = readSchemaForKeyspaces(COLUMNFAMILIES, keyspaces);
        Map<DecoratedKey, ReadPartition> newTypes = readSchemaForKeyspaces(USERTYPES, keyspaces);
        Map<DecoratedKey, ReadPartition> newFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, ReadPartition> newAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeTables(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);
        mergeAggregates(oldAggregates, newAggregates);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            Schema.instance.dropKeyspace(keyspaceToDrop);
    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, ReadPartition> before, Map<DecoratedKey, ReadPartition> after)
    {
        for (ReadPartition newPartition : after.values())
        {
            ReadPartition oldPartition = before.remove(newPartition.partitionKey());
            if (oldPartition == null || oldPartition.isEmpty())
            {
                Schema.instance.addKeyspace(createKeyspaceFromSchemaPartition(newPartition.rowIterator()));
            }
            else
            {
                String name = AsciiType.instance.compose(newPartition.partitionKey().getKey());
                Schema.instance.updateKeyspace(name);
            }
        }

        // What's remain in old is those keyspace that are not in updated, i.e. the dropped ones.
        return asKeyspaceNamesSet(before.keySet());
    }

    private static Set<String> asKeyspaceNamesSet(Set<DecoratedKey> keys)
    {
        Set<String> names = new HashSet(keys.size());
        for (DecoratedKey key : keys)
            names.add(AsciiType.instance.compose(key.getKey()));
        return names;
    }

    private static void mergeTables(Map<DecoratedKey, ReadPartition> before, Map<DecoratedKey, ReadPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropTable(oldRow.getString("keyspace_name"), oldRow.getString("columnfamily_name"));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addTable(createTableFromTableRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateTable(newRow.getString("keyspace_name"), newRow.getString("columnfamily_name"));
            }
        });
    }

    private static void mergeTypes(Map<DecoratedKey, ReadPartition> before, Map<DecoratedKey, ReadPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropType(createTypeFromRow(oldRow));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addType(createTypeFromRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateType(createTypeFromRow(newRow));
            }
        });
    }

    private static void mergeFunctions(Map<DecoratedKey, ReadPartition> before, Map<DecoratedKey, ReadPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropFunction(createFunctionFromFunctionRow(oldRow));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addFunction(createFunctionFromFunctionRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateFunction(createFunctionFromFunctionRow(newRow));
            }
        });
    }

    private static void mergeAggregates(Map<DecoratedKey, ReadPartition> before, Map<DecoratedKey, ReadPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropAggregate(createAggregateFromAggregateRow(oldRow));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addAggregate(createAggregateFromAggregateRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateAggregate(createAggregateFromAggregateRow(newRow));
            }
        });
    }

    public interface Differ
    {
        public void onDropped(UntypedResultSet.Row oldRow);
        public void onAdded(UntypedResultSet.Row newRow);
        public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow);
    }

    private static void diffSchema(Map<DecoratedKey, ReadPartition> before, Map<DecoratedKey, ReadPartition> after, Differ differ)
    {
        for (ReadPartition newPartition : after.values())
        {
            CFMetaData metadata = newPartition.metadata();
            DecoratedKey key = newPartition.partitionKey();

            ReadPartition oldPartition = before.remove(key);

            if (oldPartition == null || oldPartition.isEmpty())
            {
                // Means everything is to be added
                for (Row row : newPartition)
                    differ.onAdded(UntypedResultSet.Row.fromInternalRow(metadata, key, row));
                continue;
            }

            Iterator<Row> oldIter = oldPartition.iterator();
            Iterator<Row> newIter = newPartition.iterator();

            Row oldRow = oldIter.hasNext() ? oldIter.next() : null;
            Row newRow = newIter.hasNext() ? newIter.next() : null;
            while (oldRow != null && newRow != null)
            {
                int cmp = metadata.comparator.compare(oldRow.clustering(), newRow.clustering());
                if (cmp < 0)
                {
                    differ.onDropped(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow));
                    oldRow = oldIter.hasNext() ? oldIter.next() : null;
                }
                else if (cmp > 0)
                {

                    differ.onAdded(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow));
                    newRow = newIter.hasNext() ? newIter.next() : null;
                }
                else
                {
                    if (!oldRow.equals(newRow))
                        differ.onUpdated(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow), UntypedResultSet.Row.fromInternalRow(metadata, key, newRow));

                    oldRow = oldIter.hasNext() ? oldIter.next() : null;
                    newRow = newIter.hasNext() ? newIter.next() : null;
                }
            }

            while (oldRow != null)
            {
                differ.onDropped(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow));
                oldRow = oldIter.hasNext() ? oldIter.next() : null;
            }
            while (newRow != null)
            {
                differ.onAdded(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow));
                newRow = newIter.hasNext() ? newIter.next() : null;
            }
        }

        // What remains is those keys that were only in before.
        for (ReadPartition partition : before.values())
            for (Row row : partition)
                differ.onDropped(UntypedResultSet.Row.fromInternalRow(partition.metadata(), partition.partitionKey(), row));
    }

    /*
     * Keyspace metadata serialization/deserialization.
     */

    public static Mutation makeCreateKeyspaceMutation(KSMetaData keyspace, long timestamp)
    {
        return makeCreateKeyspaceMutation(keyspace, timestamp, true);
    }

    private static Mutation makeCreateKeyspaceMutation(KSMetaData keyspace, long timestamp, boolean withTablesAndTypesAndFunctions)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Keyspaces, timestamp, keyspace.name).clustering();

        adder.add("durable_writes", keyspace.durableWrites);
        adder.add("strategy_class", keyspace.strategyClass.getName());
        adder.add("strategy_options", json(keyspace.strategyOptions));

        Mutation mutation = adder.build();

        if (withTablesAndTypesAndFunctions)
        {
            for (UserType type : keyspace.userTypes.getAllTypes().values())
                addTypeToSchemaMutation(type, timestamp, mutation);

            for (CFMetaData table : keyspace.cfMetaData().values())
                addTableToSchemaMutation(table, timestamp, true, mutation);
        }

        return mutation;
    }

    public static Mutation makeDropKeyspaceMutation(KSMetaData keyspace, long timestamp)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        Mutation mutation = new Mutation(SystemKeyspace.NAME, getSchemaKSDecoratedKey(keyspace.name));
        for (CFMetaData schemaTable : All)
            mutation.add(PartitionUpdate.fullPartitionDelete(schemaTable, mutation.key(), timestamp, nowInSec));
        mutation.add(PartitionUpdate.fullPartitionDelete(SystemKeyspace.BuiltIndexes, mutation.key(), timestamp, nowInSec));
        return mutation;
    }

    private static KSMetaData createKeyspaceFromSchemaPartitions(RowIterator serializedKeyspace, RowIterator serializedTables, RowIterator serializedTypes)
    {
        Collection<CFMetaData> tables = createTablesFromTablesPartition(serializedTables);
        UTMetaData types = new UTMetaData(createTypesFromPartition(serializedTypes));
        return createKeyspaceFromSchemaPartition(serializedKeyspace).cloneWith(tables, types);
    }

    public static KSMetaData createKeyspaceFromName(String keyspace)
    {
        try (RowIterator partition = readSchemaPartitionForKeyspace(KEYSPACES, keyspace))
        {
            if (RowIterators.isEmpty(partition))
                throw new RuntimeException(String.format("%s not found in the schema definitions keyspaceName (%s).", keyspace, KEYSPACES));

            return createKeyspaceFromSchemaPartition(partition);
        }
    }

    /**
     * Deserialize only Keyspace attributes without nested tables or types
     *
     * @param partition Keyspace attributes in serialized form
     */
    private static KSMetaData createKeyspaceFromSchemaPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, KEYSPACES);
        UntypedResultSet.Row row = QueryProcessor.resultify(query, partition).one();
        return new KSMetaData(row.getString("keyspace_name"),
                              AbstractReplicationStrategy.getClass(row.getString("strategy_class")),
                              fromJsonMap(row.getString("strategy_options")),
                              row.getBoolean("durable_writes"));
    }

    /*
     * User type metadata serialization/deserialization.
     */

    public static Mutation makeCreateTypeMutation(KSMetaData keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addTypeToSchemaMutation(type, timestamp, mutation);
        return mutation;
    }

    private static void addTypeToSchemaMutation(UserType type, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Usertypes, timestamp, mutation)
                                 .clustering(type.name);

        adder.resetCollection("field_names");
        adder.resetCollection("field_types");
        adder.resetCollection("field_resolvers");

        for (int i = 0; i < type.size(); i++)
        {
            adder.addListEntry("field_names", type.fieldName(i));
            adder.addListEntry("field_types", type.fieldType(i).toString());
            adder.addListEntry("field_resolvers", type.fieldResolver(i).getName());
        }
        adder.build();
    }

    public static Mutation dropTypeFromSchemaMutation(KSMetaData keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        return RowUpdateBuilder.deleteRow(Usertypes, timestamp, mutation, type.name);
    }

    private static Map<ByteBuffer, UserType> createTypesFromPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, USERTYPES);
        Map<ByteBuffer, UserType> types = new HashMap<>();
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            UserType type = createTypeFromRow(row);
            types.put(type.name, type);
        }
        return types;
    }

    private static UserType createTypeFromRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        ByteBuffer name = ByteBufferUtil.bytes(row.getString("type_name"));
        List<String> rawColumns = row.getList("field_names", UTF8Type.instance);
        List<String> rawTypes = row.getList("field_types", UTF8Type.instance);
        List<String> rawResolvers = row.getList("field_resolvers", UTF8Type.instance);

        List<ByteBuffer> columns = new ArrayList<>(rawColumns.size());
        for (String rawColumn : rawColumns)
            columns.add(ByteBufferUtil.bytes(rawColumn));

        List<AbstractType<?>> types = new ArrayList<>(rawTypes.size());
        for (String rawType : rawTypes)
            types.add(parseType(rawType));

        List<Resolver> resolvers = new ArrayList<>(rawTypes.size());
        for (String rawResolver: rawResolvers)
            resolvers.add(CellResolver.getResolver(rawResolver));

        return new UserType(keyspace, name, columns, types, resolvers);
    }

    /*
     * Table metadata serialization/deserialization.
     */

    public static Mutation makeCreateTableMutation(KSMetaData keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addTableToSchemaMutation(table, timestamp, true, mutation);
        return mutation;
    }

    private static void addTableToSchemaMutation(CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        RowUpdateBuilder adder = new RowUpdateBuilder(Columnfamilies, timestamp, mutation)
                                 .clustering(table.cfName);

        adder.add("cf_id", table.cfId);
        adder.add("type", table.isSuper() ? "Super" : "Standard");

        if (table.isSuper())
        {
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            adder.add("comparator", table.comparator.subtype(0).toString());
            adder.add("subcomparator", table.comparator.subtype(1).toString());
        }
        else
        {
            adder.add("comparator", LegacyLayout.makeLegacyComparator(table).toString());
        }

        adder.add("bloom_filter_fp_chance", table.getBloomFilterFpChance());
        adder.add("caching", table.getCaching().toString());
        adder.add("comment", table.getComment());
        adder.add("compaction_strategy_class", table.compactionStrategyClass.getName());
        adder.add("compaction_strategy_options", json(table.compactionStrategyOptions));
        adder.add("compression_parameters", json(table.compressionParameters.asThriftOptions()));
        adder.add("default_time_to_live", table.getDefaultTimeToLive());
        adder.add("gc_grace_seconds", table.getGcGraceSeconds());
        adder.add("key_validator", table.getKeyValidator().toString());
        adder.add("local_read_repair_chance", table.getDcLocalReadRepairChance());
        adder.add("max_compaction_threshold", table.getMaxCompactionThreshold());
        adder.add("max_index_interval", table.getMaxIndexInterval());
        adder.add("memtable_flush_period_in_ms", table.getMemtableFlushPeriod());
        adder.add("min_compaction_threshold", table.getMinCompactionThreshold());
        adder.add("min_index_interval", table.getMinIndexInterval());
        adder.add("read_repair_chance", table.getReadRepairChance());
        adder.add("speculative_retry", table.getSpeculativeRetry().toString());

        for (Map.Entry<ColumnIdentifier, CFMetaData.DroppedColumn> entry : table.getDroppedColumns().entrySet())
        {
            String name = entry.getKey().toString();
            CFMetaData.DroppedColumn column = entry.getValue();
            adder.addMapEntry("dropped_columns", name, column.droppedTime);
            if (column.type != null)
                adder.addMapEntry("dropped_columns_types", name, column.type.toString());
        }

        adder.add("is_dense", table.isDense());

        adder.add("default_validator", table.makeLegacyDefaultValidator().toString());

        if (withColumnsAndTriggers)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, timestamp, mutation);

            for (TriggerDefinition trigger : table.getTriggers().values())
                addTriggerToSchemaMutation(table, trigger, timestamp, mutation);
        }

        adder.build();
    }

    public static Mutation makeUpdateTableMutation(KSMetaData keyspace,
                                                   CFMetaData oldTable,
                                                   CFMetaData newTable,
                                                   long timestamp,
                                                   boolean fromThrift)
    {
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        addTableToSchemaMutation(newTable, timestamp, false, mutation);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(oldTable.getColumnMetadata(),
                                                                                 newTable.getColumnMetadata());

        // columns that are no longer needed
        for (ColumnDefinition column : columnDiff.entriesOnlyOnLeft().values())
        {
            // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
            // are being deleted just because they are not here.
            if (fromThrift && column.kind != ColumnDefinition.Kind.REGULAR)
                continue;

            dropColumnFromSchemaMutation(oldTable, column, timestamp, mutation);
        }

        // newly added columns
        for (ColumnDefinition column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newTable, column, timestamp, mutation);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newTable, newTable.getColumnDefinition(name), timestamp, mutation);

        MapDifference<String, TriggerDefinition> triggerDiff = Maps.difference(oldTable.getTriggers(), newTable.getTriggers());

        // dropped triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, timestamp, mutation);

        // newly created triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, timestamp, mutation);

        return mutation;
    }

    public static Mutation makeDropTableMutation(KSMetaData keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        RowUpdateBuilder.deleteRow(Columnfamilies, timestamp, mutation, table.cfName);

        for (ColumnDefinition column : table.allColumns())
            dropColumnFromSchemaMutation(table, column, timestamp, mutation);

        for (TriggerDefinition trigger : table.getTriggers().values())
            dropTriggerFromSchemaMutation(table, trigger, timestamp, mutation);

        // TODO: get rid of in #6717
        for (String indexName : Keyspace.open(keyspace.name).getColumnFamilyStore(table.cfName).getBuiltIndexes())
            RowUpdateBuilder.deleteRow(SystemKeyspace.BuiltIndexes, timestamp, mutation, indexName);

        return mutation;
    }

    public static CFMetaData createTableFromName(String keyspace, String table)
    {
        try (RowIterator partition = readSchemaPartitionForTable(COLUMNFAMILIES, keyspace, table))
        {
            if (RowIterators.isEmpty(partition))
                throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspace, table));

            return createTableFromTablePartition(partition);
        }
    }

    /**
     * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the table and its metadata for faster lookup
     */
    private static Collection<CFMetaData> createTablesFromTablesPartition(RowIterator partition)
    {
        if (RowIterators.isEmpty(partition))
            return Collections.emptyList();

        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        List<CFMetaData> tables = new ArrayList<>();
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
            tables.add(createTableFromTableRow(row));
        return tables;
    }

    public static CFMetaData createTableFromTablePartitionAndColumnsPartition(RowIterator serializedTable, RowIterator serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRowAndColumnsPartition(QueryProcessor.resultify(query, serializedTable).one(), serializedColumns);
    }

    private static CFMetaData createTableFromTableRowAndColumnsPartition(UntypedResultSet.Row tableRow, RowIterator serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNS);
        return createTableFromTableRowAndColumnRows(tableRow, QueryProcessor.resultify(query, serializedColumns));
    }

    private static CFMetaData createTableFromTablePartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRow(QueryProcessor.resultify(query, partition).one());
    }

    /**
     * Deserialize table metadata from low-level representation
     *
     * @return Metadata deserialized from schema
     */
    private static CFMetaData createTableFromTableRow(UntypedResultSet.Row result)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        try (RowIterator serializedColumns = readSchemaPartitionForTable(COLUMNS, ksName, cfName);
             RowIterator serializedTriggers = readSchemaPartitionForTable(TRIGGERS, ksName, cfName))
        {
            CFMetaData cfm = createTableFromTableRowAndColumnsPartition(result, serializedColumns);
            for (TriggerDefinition trigger : createTriggersFromTriggersPartition(serializedTriggers))
                cfm.addTriggerDefinition(trigger);

            return cfm;
        }
    }

    public static CFMetaData createTableFromTableRowAndColumnRows(UntypedResultSet.Row result,
                                                                  UntypedResultSet serializedColumnDefinitions)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
        AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;
        Resolver resolver = CellResolver.getResolver(null);

        boolean isSuper = result.getString("type").toLowerCase().equals("super");
        boolean isDense = result.getBoolean("is_dense");
        boolean isCompound = rawComparator instanceof CompositeType;

        // We don't really use the default validator but as we have it for backward compatibility, we use it to know if it's a counter table
        boolean isCounter = TypeParser.parse(result.getString("default_validator")) instanceof CounterColumnType;

        // if we are upgrading, we use id generated from names initially
        UUID cfId = result.has("cf_id")
                  ? result.getUUID("cf_id")
                  : CFMetaData.generateLegacyCfId(ksName, cfName);

        List<ColumnDefinition> columnDefs = createColumnsFromColumnRows(serializedColumnDefinitions,
                                                                        ksName,
                                                                        cfName,
                                                                        rawComparator,
                                                                        subComparator,
                                                                        isSuper,
                                                                        isDense);

        CFMetaData cfm = CFMetaData.create(ksName, cfName, cfId, isDense, isCompound, isSuper, isCounter, columnDefs);
        cfm.columnNameComparator = isDense ? UTF8Type.instance : getColumnNameComparator(rawComparator, columnDefs);

        cfm.readRepairChance(result.getDouble("read_repair_chance"));
        cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
        cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
        cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
        cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
        if (result.has("comment"))
            cfm.comment(result.getString("comment"));
        if (result.has("memtable_flush_period_in_ms"))
            cfm.memtableFlushPeriod(result.getInt("memtable_flush_period_in_ms"));
        cfm.caching(CachingOptions.fromString(result.getString("caching")));
        if (result.has("default_time_to_live"))
            cfm.defaultTimeToLive(result.getInt("default_time_to_live"));
        if (result.has("speculative_retry"))
            cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(result.getString("speculative_retry")));
        cfm.compactionStrategyClass(CFMetaData.createCompactionStrategy(result.getString("compaction_strategy_class")));
        cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
        cfm.compactionStrategyOptions(fromJsonMap(result.getString("compaction_strategy_options")));

        if (result.has("min_index_interval"))
            cfm.minIndexInterval(result.getInt("min_index_interval"));

        if (result.has("max_index_interval"))
            cfm.maxIndexInterval(result.getInt("max_index_interval"));

        if (result.has("bloom_filter_fp_chance"))
            cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
        else
            cfm.bloomFilterFpChance(cfm.getBloomFilterFpChance());

        if (result.has("dropped_columns"))
        {
            Map<String, String> types = result.has("dropped_columns_types")
                                      ? result.getMap("dropped_columns_types", UTF8Type.instance, UTF8Type.instance) 
                                      : Collections.<String, String>emptyMap();
            addDroppedColumns(cfm, result.getMap("dropped_columns", UTF8Type.instance, LongType.instance), types);
        }

        return cfm;
    }

    private static AbstractType<?> getColumnNameComparator(AbstractType<?> rawComparator, List<ColumnDefinition> defs)
    {
        for (ColumnDefinition def : defs)
        {
            if (def.isRegular())
                return getComponentComparator(rawComparator, def.isOnAllComponents() ? null : def.position());
        }
        return UTF8Type.instance;
    }

    private static void addDroppedColumns(CFMetaData cfm, Map<String, Long> droppedTimes, Map<String, String> types)
    {
        for (Map.Entry<String, Long> entry : droppedTimes.entrySet())
        {
            String name = entry.getKey();
            long time = entry.getValue();
            AbstractType<?> type = types.containsKey(name) ? TypeParser.parse(types.get(name)) : null;
            cfm.getDroppedColumns().put(new ColumnIdentifier(name, true), new CFMetaData.DroppedColumn(type, time));
        }
    }

    /*
     * Column metadata serialization/deserialization.
     */

    private static void addColumnToSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Columns, timestamp, mutation)
                                 .clustering(table.cfName, column.name.toString());

        adder.add("validator", column.type.toString());
        adder.add("type", serializeKind(column.kind, table.isDense()));
        adder.add("component_index", column.isOnAllComponents() ? null : column.position());
        adder.add("index_name", column.getIndexName());
        adder.add("index_type", column.getIndexType() == null ? null : column.getIndexType().toString());
        adder.add("index_options", json(column.getIndexOptions()));
        adder.add("column_resolver", column.getResolver().getName());

        adder.build();
    }

    private static String serializeKind(ColumnDefinition.Kind kind, boolean isDense)
    {
        // For backward compatibility, we special case CLUSTERING_COLUMN and the case where the table is dense.
        if (kind == ColumnDefinition.Kind.CLUSTERING_COLUMN)
            return "clustering_key";

        if (kind == ColumnDefinition.Kind.REGULAR && isDense)
            return "compact_value";

        return kind.toString().toLowerCase();
    }

    private static ColumnDefinition.Kind deserializeKind(String kind)
    {
        if (kind.equalsIgnoreCase("clustering_key"))
            return ColumnDefinition.Kind.CLUSTERING_COLUMN;
        if (kind.equalsIgnoreCase("compact_value"))
            return ColumnDefinition.Kind.REGULAR;
        return Enum.valueOf(ColumnDefinition.Kind.class, kind.toUpperCase());
    }

    private static void dropColumnFromSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        RowUpdateBuilder.deleteRow(Columns, timestamp, mutation, table.cfName, column.name.toString());
    }

    private static List<ColumnDefinition> createColumnsFromColumnRows(UntypedResultSet rows,
                                                                      String keyspace,
                                                                      String table,
                                                                      AbstractType<?> rawComparator,
                                                                      AbstractType<?> rawSubComparator,
                                                                      boolean isSuper,
                                                                      boolean isDense)
    {
        List<ColumnDefinition> columns = new ArrayList<>();
        for (UntypedResultSet.Row row : rows) {
            columns.add(createColumnFromColumnRow(row, keyspace, table, rawComparator, rawSubComparator, isSuper, isDense));
        }

        return columns;
    }

    private static ColumnDefinition createColumnFromColumnRow(UntypedResultSet.Row row,
                                                              String keyspace,
                                                              String table,
                                                              AbstractType<?> rawComparator,
                                                              AbstractType<?> rawSubComparator,
                                                              boolean isSuper,
                                                              boolean isDense)
    {
        ColumnDefinition.Kind kind = deserializeKind(row.getString("type"));

        Integer componentIndex = null;
        if (row.has("component_index"))
            componentIndex = row.getInt("component_index");
        else if (kind == ColumnDefinition.Kind.CLUSTERING_COLUMN && isSuper)
            componentIndex = 1; // A ColumnDefinition for super columns applies to the column component

        // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
        // we need to use the comparator fromString method
        AbstractType<?> comparator = isSuper ? rawSubComparator : (!isDense && kind == ColumnDefinition.Kind.REGULAR
                                                                   ? getComponentComparator(rawComparator, componentIndex)
                                                                   : UTF8Type.instance);
        ColumnIdentifier name = new ColumnIdentifier(comparator.fromString(row.getString("column_name")), comparator);

        AbstractType<?> validator = parseType(row.getString("validator"));
        Resolver resolver = row.has("column_resolver") ?
                CellResolver.getResolver(row.getString("column_resolver")) :
                CellResolver.getResolver(null);

        IndexType indexType = null;
        if (row.has("index_type"))
            indexType = IndexType.valueOf(row.getString("index_type"));

        Map<String, String> indexOptions = null;
        if (row.has("index_options"))
            indexOptions = fromJsonMap(row.getString("index_options"));

        String indexName = null;
        if (row.has("index_name"))
            indexName = row.getString("index_name");

        return new ColumnDefinition(keyspace, table, name, validator, indexType, indexOptions, indexName, componentIndex, kind, resolver);
    }

    private static AbstractType<?> getComponentComparator(AbstractType<?> rawComparator, Integer componentIndex)
    {
        return (componentIndex == null || (componentIndex == 0 && !(rawComparator instanceof CompositeType)))
                ? rawComparator
                : ((CompositeType)rawComparator).types.get(componentIndex);
    }

    /*
     * Trigger metadata serialization/deserialization.
     */

    private static void addTriggerToSchemaMutation(CFMetaData table, TriggerDefinition trigger, long timestamp, Mutation mutation)
    {
        new RowUpdateBuilder(Triggers, timestamp, mutation)
            .clustering(table.cfName, trigger.name)
            .addMapEntry("trigger_options", "class", trigger.classOption)
            .build();
    }

    private static void dropTriggerFromSchemaMutation(CFMetaData table, TriggerDefinition trigger, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder.deleteRow(Triggers, timestamp, mutation, table.cfName, trigger.name);
    }

    /**
     * Deserialize triggers from storage-level representation.
     *
     * @param partition storage-level partition containing the trigger definitions
     * @return the list of processed TriggerDefinitions
     */
    private static List<TriggerDefinition> createTriggersFromTriggersPartition(RowIterator partition)
    {
        List<TriggerDefinition> triggers = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, TRIGGERS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            String name = row.getString("trigger_name");
            String classOption = row.getMap("trigger_options", UTF8Type.instance, UTF8Type.instance).get("class");
            triggers.add(new TriggerDefinition(name, classOption));
        }
        return triggers;
    }

    /*
     * UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateFunctionMutation(KSMetaData keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addFunctionToSchemaMutation(function, timestamp, mutation);
        return mutation;
    }

    private static void addFunctionToSchemaMutation(UDFunction function, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Functions, timestamp, mutation)
                                 .clustering(function.name().name, functionSignatureWithTypes(function));

        adder.add("body", function.body());
        adder.add("is_deterministic", function.isDeterministic());
        adder.add("language", function.language());
        adder.add("return_type", function.returnType().toString());

        adder.resetCollection("argument_names");
        adder.resetCollection("argument_types");
        for (int i = 0; i < function.argNames().size(); i++)
        {
            adder.addListEntry("argument_names", function.argNames().get(i).bytes);
            adder.addListEntry("argument_types", function.argTypes().get(i).toString());
        }
        adder.build();
    }

    public static Mutation makeDropFunctionMutation(KSMetaData keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        return RowUpdateBuilder.deleteRow(Functions, timestamp, mutation, function.name().name, functionSignatureWithTypes(function));
    }

    private static Collection<UDFunction> createFunctionsFromFunctionsPartition(RowIterator partition)
    {
        List<UDFunction> functions = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, FUNCTIONS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
            functions.add(createFunctionFromFunctionRow(row));
        return functions;
    }

    private static UDFunction createFunctionFromFunctionRow(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("function_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<ColumnIdentifier> argNames = new ArrayList<>();
        if (row.has("argument_names"))
            for (String arg : row.getList("argument_names", UTF8Type.instance))
                argNames.add(new ColumnIdentifier(arg, true));

        List<AbstractType<?>> argTypes = new ArrayList<>();
        if (row.has("argument_types"))
            for (String type : row.getList("argument_types", UTF8Type.instance))
                argTypes.add(parseType(type));

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        boolean isDeterministic = row.getBoolean("is_deterministic");
        String language = row.getString("language");
        String body = row.getString("body");

        try
        {
            return UDFunction.create(name, argNames, argTypes, returnType, language, body, isDeterministic);
        }
        catch (InvalidRequestException e)
        {
            logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", name), e);
            return UDFunction.createBrokenFunction(name, argNames, argTypes, returnType, language, body, e);
        }
    }

    /*
     * Aggregate UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateAggregateMutation(KSMetaData keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addAggregateToSchemaMutation(aggregate, timestamp, mutation);
        return mutation;
    }

    private static void addAggregateToSchemaMutation(UDAggregate aggregate, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Aggregates, timestamp, mutation)
                                 .clustering(aggregate.name().name, functionSignatureWithTypes(aggregate));

        adder.resetCollection("argument_types");
        adder.add("return_type", aggregate.returnType().toString());
        adder.add("state_func", aggregate.stateFunction().name().name);
        if (aggregate.stateType() != null)
            adder.add("state_type", aggregate.stateType().toString());
        if (aggregate.finalFunction() != null)
            adder.add("final_func", aggregate.finalFunction().name().name);
        if (aggregate.initialCondition() != null)
            adder.add("initcond", aggregate.initialCondition());

        for (AbstractType<?> argType : aggregate.argTypes())
            adder.addListEntry("argument_types", argType.toString());

        adder.build();
    }

    private static Collection<UDAggregate> createAggregatesFromAggregatesPartition(RowIterator partition)
    {
        List<UDAggregate> aggregates = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, AGGREGATES);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
            aggregates.add(createAggregateFromAggregateRow(row));
        return aggregates;
    }

    private static UDAggregate createAggregateFromAggregateRow(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("aggregate_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<AbstractType<?>> argTypes;
        if (types == null)
        {
            argTypes = Collections.emptyList();
        }
        else
        {
            argTypes = new ArrayList<>(types.size());
            for (String type : types)
                argTypes.add(parseType(type));
        }

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        FunctionName stateFunc = new FunctionName(ksName, row.getString("state_func"));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(ksName, row.getString("final_func")) : null;
        AbstractType<?> stateType = row.has("state_type") ? parseType(row.getString("state_type")) : null;
        ByteBuffer initcond = row.has("initcond") ? row.getBytes("initcond") : null;

        try
        {
            return UDAggregate.create(name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
        }
        catch (InvalidRequestException reason)
        {
            return UDAggregate.createBroken(name, argTypes, returnType, initcond, reason);
        }
    }

    public static Mutation makeDropAggregateMutation(KSMetaData keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        return RowUpdateBuilder.deleteRow(Aggregates, timestamp, mutation, aggregate.name().name, functionSignatureWithTypes(aggregate));
    }

    private static AbstractType<?> parseType(String str)
    {
        return TypeParser.parse(str);
    }

    // We allow method overloads, so a function is not uniquely identified by its name only, but
    // also by its argument types. To distinguish overloads of given function name in the schema
    // we use a "signature" which is just a list of it's CQL argument types (we could replace that by
    // using a "signature" UDT that would be comprised of the function name and argument types,
    // which we could then use as clustering column. But as we haven't yet used UDT in system tables,
    // We'll leave that decision to #6717).
    public static ByteBuffer functionSignatureWithTypes(AbstractFunction fun)
    {
        ListType<String> list = ListType.getInstance(UTF8Type.instance, false);
        List<String> strList = new ArrayList<>(fun.argTypes().size());
        for (AbstractType<?> argType : fun.argTypes())
            strList.add(argType.asCQL3Type().toString());
        return list.decompose(strList);
    }

    public static ByteBuffer functionSignatureWithNameAndTypes(AbstractFunction fun)
    {
        ListType<String> list = ListType.getInstance(UTF8Type.instance, false);
        List<String> strList = new ArrayList<>(fun.argTypes().size() + 2);
        strList.add(fun.name().keyspace);
        strList.add(fun.name().name);
        for (AbstractType<?> argType : fun.argTypes())
            strList.add(argType.asCQL3Type().toString());
        return list.decompose(strList);
    }
}
