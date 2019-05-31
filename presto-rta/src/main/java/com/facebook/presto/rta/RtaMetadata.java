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
package com.facebook.presto.rta;

import com.facebook.presto.aresdb.AresDbColumnHandle;
import com.facebook.presto.aresdb.AresDbConnectorId;
import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.rta.schema.RTADeployment;
import com.facebook.presto.rta.schema.RTASchemaHandler;
import com.facebook.presto.rta.schema.RTATableEntity;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorTablePartitioning;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.rta.RtaUtil.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RtaMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RtaMetadata.class);

    private final RtaConnectorId connectorId;
    private final RTASchemaHandler schemaHandler;
    private final RtaConnectorProvider connectorProvider;
    private final RtaPropertyManager propertyManager;

    @Inject
    public RtaMetadata(RtaConnectorId connectorId, RTASchemaHandler schemaHandler, RtaConnectorProvider connectorProvider, RtaPropertyManager propertyManager)
    {
        this.connectorId = connectorId;
        this.schemaHandler = schemaHandler;
        this.connectorProvider = connectorProvider;
        this.propertyManager = propertyManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    private List<String> listSchemaNames()
    {
        return schemaHandler.getAllNamespaces();
    }

    @Override
    public RtaTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        RTATableEntity entity = schemaHandler.getEntity(tableName.getSchemaName(), tableName.getTableName());
        RTADeployment deployment = propertyManager.getDefaultDeployment(entity);

        // Check if the datacenter field I added in the deployment is actually exposed by the API
        RtaStorageKey key = RtaStorageKey.fromDeployment(deployment);

        ConnectorTableHandle underlyingHandle;
        switch (key.getType()) {
            case ARESDB:
                underlyingHandle = new AresDbTableHandle(new AresDbConnectorId(connectorId.getId()), entity.getTable(), entity.getTimestampField());
                break;
            case PINOT:
                underlyingHandle = new PinotTableHandle(connectorId.getId(), entity.getTable(), entity.getTable());
                break;
            default:
                throw new IllegalStateException("Unknown connector type " + key.getType());
        }
        return new RtaTableHandle(connectorId, key, tableName, underlyingHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Collection<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        }
        else {
            schemaNames = listSchemaNames();
        }

        return schemaNames.stream().flatMap(
                schema -> schemaHandler.getTablesInNamespace(schema).stream().map(table -> new SchemaTableName(schema, table.toLowerCase(ENGLISH)))).collect(toImmutableList());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        RtaTableHandle tableHandle = checkType(table, RtaTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new RtaTableLayoutHandle(tableHandle, Optional.of(constraint.getSummary()), Optional.empty()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        RtaTableLayoutHandle rtaTableLayoutHandle = (RtaTableLayoutHandle) handle;
        ConnectorMetadata metadata = connectorProvider.getConnector(rtaTableLayoutHandle.getTable().getKey()).getMetadata(RtaTransactionHandle.INSTANCE);
        ConnectorTableLayout tableLayout = metadata.getTableLayout(session, rtaTableLayoutHandle.createConnectorSpecificTableLayoutHandle());
        Optional<ConnectorTablePartitioning> tablePartitioning = tableLayout.getTablePartitioning().map(underlyingPartitioningHandle -> new ConnectorTablePartitioning(new RtaPartitioningHandle(rtaTableLayoutHandle.getTable().getKey(), underlyingPartitioningHandle.getPartitioningHandle()), underlyingPartitioningHandle.getPartitioningColumns()));
        return new ConnectorTableLayout(rtaTableLayoutHandle, tableLayout.getColumns(), tableLayout.getPredicate(), tableLayout.getCompactEffectivePredicate(), tablePartitioning, tableLayout.getStreamPartitioningColumns(), tableLayout.getDiscretePredicates(), tableLayout.getLocalProperties());
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, schemaHandler.getEntity(tableName.getSchemaName(), tableName.getTableName()).getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        RtaTableHandle rtaTableHandle = checkType(table, RtaTableHandle.class, "table");
        checkArgument(rtaTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = rtaTableHandle.getSchemaTableName();
        return getTableMetadata(tableName);
    }

    private static ColumnHandle createColumnHandleOfSpecificType(RtaStorageType storageType, String name, Type type)
    {
        switch (storageType) {
            case PINOT:
                return new PinotColumnHandle(name, type, PinotColumnHandle.PinotColumnType.REGULAR);
            case ARESDB:
                return new AresDbColumnHandle(name, type, AresDbColumnHandle.AresDbColumnType.REGULAR);
            default:
                throw new IllegalStateException("Invalid underlying handle of type " + storageType);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RtaTableHandle rtaTableHandle = checkType(tableHandle, RtaTableHandle.class, "tableHandle");
        checkArgument(rtaTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = rtaTableHandle.getSchemaTableName();
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata column : schemaHandler.getEntity(tableName.getSchemaName(), tableName.getTableName()).getColumnsMetadata()) {
            String originalName = ((RtaColumnMetadata) column).getRtaName();
            Type type = column.getType();
            columnHandles.put(originalName.toLowerCase(ENGLISH), createColumnHandleOfSpecificType(rtaTableHandle.getKey().getType(), originalName, type));
        }
        return columnHandles.build();
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

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, RtaTableHandle.class, "tableHandle");
        if (columnHandle instanceof PinotColumnHandle) {
            return ((PinotColumnHandle) columnHandle).getColumnMetadata();
        }
        else if (columnHandle instanceof AresDbColumnHandle) {
            return ((AresDbColumnHandle) columnHandle).getColumnMetadata();
        }
        else {
            throw new IllegalStateException("Unknown column handle type " + columnHandle + " of type " + (columnHandle == null ? "null" : columnHandle.getClass()));
        }
    }

    @Override
    public Optional<TableScanPipeline> convertToTableScanPipeline(ConnectorSession session, ConnectorTableHandle tableHandle, TablePipelineNode tablePipelineNode)
    {
        TablePipelineNode underlyingConnectorTablePipelineNode = new TablePipelineNode((
                (RtaTableHandle) tableHandle).getHandle(),
                tablePipelineNode.getInputColumns(),
                tablePipelineNode.getOutputColumns(),
                tablePipelineNode.getRowType());
        TableScanPipeline scanPipeline = new TableScanPipeline();
        scanPipeline.addPipeline(underlyingConnectorTablePipelineNode,
                underlyingConnectorTablePipelineNode.getInputColumns());

        return Optional.of(scanPipeline);
    }

    @Override
    public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
            TableScanPipeline currentPipeline, ProjectPipelineNode project)
    {
        RtaTableHandle rtaTableHandle = (RtaTableHandle) connectorTableHandle;
        ConnectorMetadata metadata = connectorProvider.getConnector(rtaTableHandle.getKey()).getMetadata(RtaTransactionHandle.INSTANCE);
        return metadata.pushProjectIntoScan(session, rtaTableHandle.getHandle(), currentPipeline, project);
    }

    @Override
    public Optional<TableScanPipeline> pushFilterIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
            TableScanPipeline currentPipeline, FilterPipelineNode filter)
    {
        RtaTableHandle rtaTableHandle = (RtaTableHandle) connectorTableHandle;
        ConnectorMetadata metadata = connectorProvider.getConnector(rtaTableHandle.getKey()).getMetadata(RtaTransactionHandle.INSTANCE);
        return metadata.pushFilterIntoScan(session, rtaTableHandle.getHandle(), currentPipeline, filter);
    }

    @Override
    public Optional<TableScanPipeline> pushAggregationIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
            TableScanPipeline currentPipeline, AggregationPipelineNode aggregation)
    {
        RtaTableHandle rtaTableHandle = (RtaTableHandle) connectorTableHandle;
        ConnectorMetadata metadata = connectorProvider.getConnector(rtaTableHandle.getKey()).getMetadata(RtaTransactionHandle.INSTANCE);
        return metadata.pushAggregationIntoScan(session, rtaTableHandle.getHandle(), currentPipeline, aggregation);
    }

    @Override
    public Optional<TableScanPipeline> pushLimitIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, LimitPipelineNode limit)
    {
        RtaTableHandle rtaTableHandle = (RtaTableHandle) connectorTableHandle;
        ConnectorMetadata metadata = connectorProvider.getConnector(rtaTableHandle.getKey()).getMetadata(RtaTransactionHandle.INSTANCE);
        return metadata.pushLimitIntoScan(session, rtaTableHandle.getHandle(), currentPipeline, limit);
    }

    @Override
    public Optional<ConnectorTableLayoutHandle> pushTableScanIntoConnectorLayoutHandle(ConnectorSession session, TableScanPipeline scanPipeline, ConnectorTableLayoutHandle
            connectorTableLayoutHandle)
    {
        RtaTableLayoutHandle currentHandle = (RtaTableLayoutHandle) connectorTableLayoutHandle;
        checkArgument(!currentHandle.getScanPipeline().isPresent(), "layout already has a scan pipeline");

        return Optional.of(new RtaTableLayoutHandle(currentHandle.getTable(), currentHandle.getConstraint(), Optional.of(scanPipeline)));
    }
}
