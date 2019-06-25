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
package com.facebook.presto.aresdb;

import com.facebook.presto.aresdb.query.AresDbQueryGenerator;
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
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbColumnHandle.AresDbColumnType.DERIVED;
import static com.facebook.presto.aresdb.AresDbColumnHandle.AresDbColumnType.REGULAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(ConnectorMetadata.class);

    private final AresDbConnectorId connectorId;
    private final AresDbConnection aresDbConnection;
    private final AresDbConfig aresDbConfig;

    @Inject
    public AresDbMetadata(AresDbConnectorId connectorId, AresDbConnection aresDbConnection, AresDbConfig aresDbConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.aresDbConnection = requireNonNull(aresDbConnection, "aresDbConnection is null");
        this.aresDbConfig = requireNonNull(aresDbConfig, "aresDbConfig is null");
    }

    private Optional<TableScanPipeline> tryCreatingNewPipeline(Supplier<Boolean> enabled, TableScanPipeline scanPipeline, PipelineNode newPipelineNode)
    {
        if (!enabled.get()) {
            return Optional.empty();
        }

        try {
            TableScanPipeline newPipeline = new TableScanPipeline(new ArrayList<>(scanPipeline.getPipelineNodes()), scanPipeline.getOutputColumnHandles());
            newPipeline.addPipeline(newPipelineNode, createDerivedColumnHandles(newPipelineNode));
            AresDbQueryGenerator.generate(newPipeline, Optional.empty(), Optional.of(aresDbConfig), Optional.empty());
            return Optional.of(newPipeline);
        }
        catch (Exception e) {
            // Adding the new node is not allowed as we fail to generate PQL
            log.debug("Pushdown failed: " + e.getMessage(), e);
            return Optional.empty();
        }
    }

    @VisibleForTesting
    static List<ColumnHandle> createDerivedColumnHandles(PipelineNode pipelineNode)
    {
        List<ColumnHandle> outputColumnHandles = new ArrayList<>();
        List<String> outputColumns = pipelineNode.getOutputColumns();
        List<Type> outputColumnTypes = pipelineNode.getRowType();

        for (int fieldId = 0; fieldId < outputColumns.size(); fieldId++) {
            // Create a column handle for the new output column
            outputColumnHandles.add(new AresDbColumnHandle(outputColumns.get(fieldId), outputColumnTypes.get(fieldId), DERIVED));
        }

        return outputColumnHandles;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        // Just expose one schema which contains all the tables. AresDb has no concept of schemas, it seems??
        return ImmutableList.of(connectorId.getId());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new AresDbTableHandle(connectorId, tableName.getTableName(), aresDbConnection.getTimeColumn(tableName.getTableName()), Optional.empty(), Optional.empty());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        AresDbTableHandle aresDbTableHandle = (AresDbTableHandle) table; // TODO: type check
        ConnectorTableLayout layout = new ConnectorTableLayout(new AresDbTableLayoutHandle(aresDbTableHandle, Optional.of(constraint.getSummary()), Optional.empty()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AresDbTableHandle aresDbTableHandle = (AresDbTableHandle) tableHandle; // TODO: type check
        AresDbTable aresDbTable = aresDbConnection.getTable(aresDbTableHandle.getTableName());

        return new ConnectorTableMetadata(schemaTableName(aresDbTableHandle.getTableName()), aresDbTable.getColumnsMetadata());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AresDbTableHandle aresDbTableHandle = (AresDbTableHandle) tableHandle; // TODO: type check
        AresDbTable aresDbTable = aresDbConnection.getTable(aresDbTableHandle.getTableName());

        ImmutableMap.Builder columnHandleMap = ImmutableMap.builder();
        for (int i = 0; i < aresDbTable.getColumns().size(); i++) {
            AresDbTable.AresDbColumn aresDbColumn = aresDbTable.getColumns().get(i);
            AresDbColumnHandle columnHandle = new AresDbColumnHandle(aresDbColumn.getName(), aresDbColumn.getDataType(), REGULAR);
            columnHandleMap.put(aresDbColumn.getName().toLowerCase(ENGLISH), columnHandle);
        }

        return columnHandleMap.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        AresDbColumnHandle aresDbColumnHandle = (AresDbColumnHandle) columnHandle; // TODO: type check
        return aresDbColumnHandle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> output = ImmutableMap.builder();

        for (String table : aresDbConnection.getTables()) {
            AresDbTable aresDbTable = aresDbConnection.getTable(table);
            output.put(schemaTableName(table), aresDbTable.getColumnsMetadata());
        }

        return output.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return aresDbConnection.getTables().stream()
                .map(t -> schemaTableName(t))
                .collect(Collectors.toList());
    }

    private SchemaTableName schemaTableName(String table)
    {
        return new SchemaTableName(connectorId.getId(), table);
    }

    @Override
    public Optional<TableScanPipeline> convertToTableScanPipeline(ConnectorSession session, ConnectorTableHandle tableHandle, TablePipelineNode tablePipelineNode)
    {
        TableScanPipeline scanPipeline = new TableScanPipeline();
        scanPipeline.addPipeline(tablePipelineNode,
                tablePipelineNode.getInputColumns());

        return Optional.of(scanPipeline);
    }

    @Override
    public Optional<TableScanPipeline> pushAggregationIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, AggregationPipelineNode aggregation)
    {
        return tryCreatingNewPipeline(aresDbConfig::isAggregationPushDownEnabled, currentPipeline, aggregation);
    }

    @Override
    public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, ProjectPipelineNode project)
    {
        return tryCreatingNewPipeline(aresDbConfig::isProjectPushDownEnabled, currentPipeline, project);
    }

    @Override
    public Optional<TableScanPipeline> pushFilterIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, FilterPipelineNode filter)
    {
        return tryCreatingNewPipeline(aresDbConfig::isFilterPushDownEnabled, currentPipeline, filter);
    }

    @Override
    public Optional<TableScanPipeline> pushLimitIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, LimitPipelineNode limit)
    {
        return tryCreatingNewPipeline(aresDbConfig::isLimitPushDownEnabled, currentPipeline, limit);
    }

    @Override
    public Optional<ConnectorTableLayoutHandle> pushTableScanIntoConnectorLayoutHandle(ConnectorSession session, TableScanPipeline scanPipeline, ConnectorTableLayoutHandle connectorTableLayoutHandle)
    {
        AresDbTableLayoutHandle currentHandle = (AresDbTableLayoutHandle) connectorTableLayoutHandle;
        checkArgument(!currentHandle.getScanPipeline().isPresent(), "layout already has a scan pipeline");

        return Optional.of(new AresDbTableLayoutHandle(currentHandle.getTable(), currentHandle.getConstraint(), Optional.of(scanPipeline)));
    }
}
