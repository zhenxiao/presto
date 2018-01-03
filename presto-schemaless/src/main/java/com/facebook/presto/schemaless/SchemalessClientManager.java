/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.schemaless;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.uber.schemaless.HttpSchemalessClient;
import com.uber.schemaless.IndexDefinition;
import com.uber.schemaless.IndexField;
import com.uber.schemaless.NonretryableException;
import com.uber.schemaless.RetryableException;
import com.uber.schemaless.SchemalessClient;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;

/**
 * Manages connections to the Schemaless nodes. A single Schemaless instance is served by a single connection.
 * TODO
 */
public class SchemalessClientManager
{
    private static final Logger log = Logger.get(SchemalessClientManager.class);

    private final SchemalessConnectorConfig config;
    private final Map<SchemaTableName, SchemalessTableDescription> tableDescriptionsMap;

    private Map<String, SchemalessClient> clients;

    @Inject
    public SchemalessClientManager(SchemalessConnectorConfig config, Map<SchemaTableName, SchemalessTableDescription> tableDescriptionsMap)
            throws IOException
    {
        this.config = config;
        this.tableDescriptionsMap = tableDescriptionsMap;
        this.clients = createClients();
    }

    public SchemalessConnectorConfig getConfig()
    {
        return config;
    }

    /**
     * Schema names are the data store names. A single datastore in Schemaless can have any number of index tables.
     * When connecting to Schemaless from Presto, we are using the configuration such that schema name refers to the
     * datastore name and the table name refers to the actual index table name.
     */
    public List<String> listSchemaNames()
    {
        return tableDescriptionsMap.keySet().stream().map(SchemaTableName::getSchemaName).collect(collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }

    /**
     * Fetches SchmalessTableDescription for the specified schemaName and tableName
     */
    public SchemalessTableDescription getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        try {
            for (Map.Entry<SchemaTableName, SchemalessTableDescription> entry : tableDescriptionsMap.entrySet()) {
                buildTableDefinition(entry.getKey(), entry.getValue());
            }
        }
        //TODO If we reach here, we do not know what could have caused this from schemaless. Throw the exception for stack tracing and change this to more specific exception in future.
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
        return tableDescriptionsMap.get(new SchemaTableName(schemaName, tableName));
    }

    private void buildTableDefinition(SchemaTableName schemaTableName, SchemalessTableDescription tableDescription)
            throws RetryableException, NonretryableException
    {
        SchemalessClient client = clients.get(tableDescription.getInstanceName());
        Map<String, IndexDefinition> definitionMap = client.getIndexDefinitions(config.getDefaultTimeOutInMs(), config.getDefaultClientIDPrefix(), null);
        for (IndexDefinition indexDefinition : definitionMap.values()) {
            if (indexDefinition.alias.toUpperCase().equals(schemaTableName.getTableName().toUpperCase())) {
                tableDescription.setIndexDefinition(buildSchemalessIndexDefinition(indexDefinition));
                return;
            }
        }
    }

    private SchemalessIndexDefinition buildSchemalessIndexDefinition(IndexDefinition indexDefinition)
    {
        return new SchemalessIndexDefinition(
                indexDefinition.kind,
                indexDefinition.alias,
                indexDefinition.state,
                indexDefinition.columnKey,
                indexDefinition.table,
                indexDefinition.shardField,
                indexDefinition.shardFieldExpr,
                indexDefinition.mutableShardKeyEnabled,
                indexDefinition.primaryKey,
                indexDefinition.useAddedId,
                indexDefinition.formatVersion,
                indexDefinition.columnKeys,
                buildSchemalessIndexFields(indexDefinition.fields),
                indexDefinition.compositeIndexes);
    }

    private List<SchemalessIndexField> buildSchemalessIndexFields(List<IndexField> indexFields)
    {
        ImmutableList.Builder<SchemalessIndexField> result = ImmutableList.builder();
        for (IndexField indexField : indexFields) {
            result.add(
                    new SchemalessIndexField(
                         indexField.name,
                            getPrestoType(indexField.type),
                            indexField.columnKey,
                            indexField.sqlType,
                            indexField.type,
                            indexField.columnKeys,
                            indexField.expr));
        }
        return result.build();
    }

    private Type getPrestoType(String fieldType)
    {
        //TODO handle GUId (what type is it stored as - binary or string?), Integer, IsNotNoneAndNotEmpty, String:255 ?, Integer:default(-1),
        Type prestoType;
        int indexOfSemicolon = fieldType.indexOf(':');
        String columnType = (indexOfSemicolon >= 0) ? fieldType.toLowerCase().substring(0, indexOfSemicolon) : fieldType.toLowerCase();
        switch (columnType) {
            case "bool":
            case "isnotnoneandnotempty":
            case "isnotnone":
                prestoType = BOOLEAN;
                break;
            case "string":
            case "text":
            case "datetime":
                prestoType = VARCHAR;
                break;
            case "geohash":
                prestoType = createVarcharType(12);
                break;
            case "guid":
                prestoType = createVarcharType(16);
                break;
            case "double":
            case "decimal":
                prestoType = DOUBLE;
                break;
            case "binary":
            case "jsonfield":
            case "blob":
                prestoType = VARBINARY;
                break;
            case "biginteger":
                prestoType = BIGINT;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            default:
                prestoType = VARCHAR;
                break;
        }
        return prestoType;
    }

    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptionsMap.keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }
        return builder.build();
    }

    public Map<String, SchemalessClient> getClients()
    {
        return clients;
    }

    /**
     * Create clients per instance of Schemaless. Example: All datastores/index tables in Mezzanine will use one Schemaless client.
     * TODO In future we may need to change this to one client connection per datastore/index table based on usage.
     *
     * @throws IOException
     */
    private Map<String, SchemalessClient> createClients()
            throws IOException
    {
        Map<String, SchemalessClient> clients = new HashMap<String, SchemalessClient>();
        for (SchemaTableName schemaTableName : tableDescriptionsMap.keySet()) {
            SchemalessTableDescription tableDescription = tableDescriptionsMap.get(schemaTableName);
            if (!clients.containsKey(tableDescription.getInstanceName())) {
                String appID = config.getDefaultClientIDPrefix() + "-" + tableDescription.getInstanceName();
                SchemalessClient schemalessClient = new HttpSchemalessClient.Builder(tableDescription.getHostAddress(), tableDescription.getPort(), tableDescription.getInstanceName(), tableDescription.getDataStoreName(), appID).build();
                clients.put(tableDescription.getInstanceName(), schemalessClient);
            }
        }
        return clients;
    }
}
