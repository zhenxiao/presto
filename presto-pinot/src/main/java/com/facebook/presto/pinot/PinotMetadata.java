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
package com.facebook.presto.pinot;

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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.pinot.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PinotMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(PinotMetadata.class);
    private final String connectorId;
    private final PinotConnection pinotPrestoConnection;

    @Inject
    public PinotMetadata(PinotConnectorId connectorId, PinotConnection pinotPrestoConnection)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        try {
            ImmutableList.Builder<String> schemaNamesListBuilder = ImmutableList.builder();
            for (String table : pinotPrestoConnection.getTableNames()) {
                schemaNamesListBuilder.add(table.toLowerCase());
            }
            return schemaNamesListBuilder.build();
        }
        catch (Exception e) {
            return ImmutableList.of();
        }
    }

    public String getPinotTableNameFromPrestoTableName(String prestoTableName)
    {
        try {
            for (String pinotTableName : pinotPrestoConnection.getTableNames()) {
                if (prestoTableName.equalsIgnoreCase(pinotTableName)) {
                    return pinotTableName;
                }
            }
        }
        catch (Exception e) {
        }
        return null;
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName().toLowerCase())) {
            return null;
        }
        String pinotTableName = getPinotTableNameFromPrestoTableName(tableName.getTableName());
        try {
            PinotTable table = pinotPrestoConnection.getTable(pinotTableName);
            if (table == null) {
                return null;
            }
        }
        catch (Exception e) {
            log.error("Failed to get TableHandle for table : " + tableName);
            return null;
        }

        return new PinotTableHandle(connectorId, tableName.getSchemaName(), pinotTableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns, Optional<TupleDomain<List<String>>> nestedTupleDomain, Optional<Map<String, List<String>>> aggregations)
    {
        PinotTableHandle tableHandle = checkType(table, PinotTableHandle.class, "table");
        tableHandle.setConstraintSummary(constraint.getSummary());
        ConnectorTableLayout layout = new ConnectorTableLayout(new PinotTableLayoutHandle(tableHandle, aggregations));
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
        PinotTableHandle pinotTableHandle = checkType(table, PinotTableHandle.class, "table");
        checkArgument(pinotTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(pinotTableHandle.getSchemaName(), pinotTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Collection<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = listSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        try {
            for (String table : pinotPrestoConnection.getTableNames()) {
                if (schemaNames.contains(table.toLowerCase())) {
                    builder.add(new SchemaTableName(table.toLowerCase(), table));
                }
            }
        }
        catch (Exception e) {
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = checkType(tableHandle, PinotTableHandle.class, "tableHandle");
        checkArgument(pinotTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        String pinotTableName = getPinotTableNameFromPrestoTableName(pinotTableHandle.getTableName());
        try {
            PinotTable table = pinotPrestoConnection.getTable(pinotTableName);
            if (table == null) {
                throw new TableNotFoundException(pinotTableHandle.toSchemaTableName());
            }
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            int index = 0;
            for (ColumnMetadata column : table.getColumnsMetadata()) {
                columnHandles.put(column.getName().toLowerCase(), new PinotColumnHandle(connectorId, ((PinotColumnMetadata) column).getPinotName(), column.getType(), index, Optional.empty()));
                index++;
            }
            return columnHandles.build();
        }
        catch (Exception e) {
            log.error("Failed to get ColumnHandles for table : " + pinotTableHandle.getTableName(), e);
            return null;
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }
        String pinotTableName = getPinotTableNameFromPrestoTableName(tableName.getTableName());
        try {
            PinotTable table = pinotPrestoConnection.getTable(pinotTableName);
            if (table == null) {
                return null;
            }
            return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        }
        catch (Exception e) {
            log.error("Failed to get ConnectorTableMetadata for table: " + tableName.getTableName(), e);
            return null;
        }
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, PinotTableHandle.class, "tableHandle");
        return checkType(columnHandle, PinotColumnHandle.class, "columnHandle").getColumnMetadata();
    }
}
