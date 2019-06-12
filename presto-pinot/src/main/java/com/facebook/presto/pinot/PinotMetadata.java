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

import com.facebook.presto.pinot.query.PinotQueryGenerator;
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
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.DERIVED;
import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.PinotUtils.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(PinotMetadata.class);
    private final String connectorId;
    private final PinotConnection pinotPrestoConnection;
    private final PinotConfig pinotConfig;

    @Inject
    public PinotMetadata(PinotConnectorId connectorId, PinotConnection pinotPrestoConnection, PinotConfig pinotConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    private List<String> listSchemaNames()
    {
        ImmutableList.Builder<String> schemaNamesListBuilder = ImmutableList.builder();
        for (String table : pinotPrestoConnection.getTableNames()) {
            schemaNamesListBuilder.add(table.toLowerCase(ENGLISH));
        }
        return schemaNamesListBuilder.build();
    }

    private String getPinotTableNameFromPrestoTableName(String prestoTableName)
    {
        List<String> allTables = pinotPrestoConnection.getTableNames();
        for (String pinotTableName : allTables) {
            if (prestoTableName.equalsIgnoreCase(pinotTableName)) {
                return pinotTableName;
            }
        }
        throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Unable to find the presto table " + prestoTableName + " in " + allTables);
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName().toLowerCase(ENGLISH))) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "schema " + tableName.getSchemaName() + " not found");
        }
        String pinotTableName = getPinotTableNameFromPrestoTableName(tableName.getTableName());
        try {
            PinotTable table = pinotPrestoConnection.getTable(pinotTableName);
            if (table == null) {
                throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "table " + pinotTableName + " is invalid");
            }
        }
        catch (Exception e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Failed to get TableHandle for table : " + tableName, e);
        }

        return new PinotTableHandle(connectorId, tableName.getSchemaName(), pinotTableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        PinotTableHandle tableHandle = checkType(table, PinotTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new PinotTableLayoutHandle(tableHandle, Optional.of(constraint.getSummary()), Optional.empty()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        PinotTableLayoutHandle pinotTableLayoutHandle = (PinotTableLayoutHandle) handle;
        if (PinotSessionProperties.isForceSingleNodePlan(session) && pinotTableLayoutHandle.getScanPipeline().isPresent()) {
            // create a single node only distribution
            return new ConnectorTableLayout(
                    handle,
                    Optional.empty(),
                    TupleDomain.all(),
                    Optional.of(
                            new ConnectorTablePartitioning(
                                    new PinotPartitioningHandle(true),
                                    ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty(),
                    emptyList());
        }

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
        for (String table : pinotPrestoConnection.getTableNames()) {
            if (schemaNames.contains(table.toLowerCase(ENGLISH))) {
                builder.add(new SchemaTableName(table.toLowerCase(ENGLISH), table));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = checkType(tableHandle, PinotTableHandle.class, "tableHandle");
        checkArgument(pinotTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        String pinotTableName = getPinotTableNameFromPrestoTableName(pinotTableHandle.getTableName());
        PinotTable table = pinotPrestoConnection.getTable(pinotTableName);
        if (table == null) {
            throw new TableNotFoundException(pinotTableHandle.toSchemaTableName());
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName().toLowerCase(ENGLISH),
                    new PinotColumnHandle(((PinotColumnMetadata) column).getPinotName(), column.getType(), REGULAR));
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

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }
        String pinotTableName = getPinotTableNameFromPrestoTableName(tableName.getTableName());
        PinotTable table = pinotPrestoConnection.getTable(pinotTableName);
        if (table == null) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
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

    @Override
    public Optional<TableScanPipeline> convertToTableScanPipeline(ConnectorSession session, ConnectorTableHandle tableHandle, TablePipelineNode tablePipelineNode)
    {
        TableScanPipeline scanPipeline = new TableScanPipeline();
        scanPipeline.addPipeline(tablePipelineNode,
                tablePipelineNode.getInputColumns());

        return Optional.of(scanPipeline);
    }

    @Override
    public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
            TableScanPipeline currentPipeline, ProjectPipelineNode project)
    {
        return tryCreatingNewPipeline(pinotConfig::isProjectPushDownEnabled, currentPipeline, project);
    }

    @Override
    public Optional<TableScanPipeline> pushFilterIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
            TableScanPipeline currentPipeline, FilterPipelineNode filter)
    {
        return tryCreatingNewPipeline(pinotConfig::isFilterPushDownEnabled, currentPipeline, filter);
    }

    @Override
    public Optional<TableScanPipeline> pushAggregationIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
            TableScanPipeline currentPipeline, AggregationPipelineNode aggregation)
    {
        if (aggregation.isPartial()) {
            return Optional.empty(); // partial aggregation is not supported
        }
        return tryCreatingNewPipeline(pinotConfig::isAggregationPushDownEnabled, currentPipeline, aggregation);
    }

    @Override
    public Optional<TableScanPipeline> pushLimitIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, LimitPipelineNode limit)
    {
        if (!limit.isPartial() && PinotScanParallelismFinder.canParallelize(PinotSessionProperties.isScanParallelismEnabled(session), currentPipeline)) {
            return Optional.empty();
        }
        else {
            return tryCreatingNewPipeline(pinotConfig::isLimitPushDownEnabled, currentPipeline, limit);
        }
    }

    private Optional<TableScanPipeline> tryCreatingNewPipeline(Supplier<Boolean> isEnabled, TableScanPipeline scanPipeline, PipelineNode newPipelineNode)
    {
        if (!isEnabled.get()) {
            return Optional.empty();
        }

        try {
            TableScanPipeline newPipeline = new TableScanPipeline(new ArrayList<>(scanPipeline.getPipelineNodes()), scanPipeline.getOutputColumnHandles());
            newPipeline.addPipeline(newPipelineNode, createDerivedColumnHandles(newPipelineNode));
            PinotQueryGenerator.generatePQL(newPipeline, Optional.of(pinotConfig));
            return Optional.of(newPipeline);
        }
        catch (Exception e) {
            // Adding the new node is not allowed as we fail to generate PQL
            log.debug("Pushdown failed", e);
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
            outputColumnHandles.add(new PinotColumnHandle(
                    outputColumns.get(fieldId),
                    outputColumnTypes.get(fieldId),
                    DERIVED));
        }

        return outputColumnHandles;
    }

    @Override
    public Optional<ConnectorTableLayoutHandle> pushTableScanIntoConnectorLayoutHandle(ConnectorSession session, TableScanPipeline scanPipeline, ConnectorTableLayoutHandle
            connectorTableLayoutHandle)
    {
        PinotTableLayoutHandle currentHandle = (PinotTableLayoutHandle) connectorTableLayoutHandle;
        checkArgument(!currentHandle.getScanPipeline().isPresent(), "layout already has a scan pipeline");

        return Optional.of(new PinotTableLayoutHandle(currentHandle.getTable(), currentHandle.getConstraint(), Optional.of(scanPipeline)));
    }
}
