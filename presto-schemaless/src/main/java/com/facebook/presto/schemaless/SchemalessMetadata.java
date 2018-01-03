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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.schemaless.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Manages the schemaless connector specific metadata information.
 * TODO Methods return null currently. Add implementation in next iteration.
 */
public class SchemalessMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final SchemalessClientManager clientManager;

    @Inject
    public SchemalessMetadata(SchemalessConnectorId connectorId, SchemalessClientManager clientManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.clientManager = requireNonNull(clientManager, "clientManage is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return clientManager.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        SchemalessTableDescription table = clientManager.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new SchemalessTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns, Optional<TupleDomain<List<String>>> nestedTupleDomain, Optional<Map<String, List<String>>> aggregations)
    {
        SchemalessTableHandle handle = checkType(table, SchemalessTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new SchemalessTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        SchemalessTableHandle handle = checkType(table, SchemalessTableHandle.class, "table");
        checkArgument(handle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(handle.getDataStoreName(), handle.getIndexTableName());
        return getTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return clientManager.listTables(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemalessTableHandle handle = checkType(tableHandle, SchemalessTableHandle.class, "table");
        checkArgument(handle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        SchemalessTableDescription table = clientManager.getTable(handle.getDataStoreName(), handle.getIndexTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        SchemalessIndexDefinition indexDefinition = table.getIndexDefinition();
        for (SchemalessIndexField field : indexDefinition.getFields()) {
            columnHandles.put(field.getName(), field.getColumnHandle(connectorId, index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, SchemalessTableHandle.class, "tableHandle");
        return checkType(columnHandle, SchemalessColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        SchemalessTableDescription table = clientManager.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        SchemalessIndexDefinition indexDefinition = table.getIndexDefinition();
        for (SchemalessIndexField field : indexDefinition.getFields()) {
            builder.add(field.getColumnMetadata());
        }

        return new ConnectorTableMetadata(tableName, builder.build());
    }
}
