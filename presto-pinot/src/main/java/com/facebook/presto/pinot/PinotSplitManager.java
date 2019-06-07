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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownInExpression;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.PushDownLogicalBinaryExpression;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.pipeline.TableScanPipelineVisitor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.linkedin.pinot.client.PinotClientException;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotSplit.createBrokerSplit;
import static com.facebook.presto.pinot.PinotSplit.createSegmentSplit;
import static com.facebook.presto.pinot.PinotUtils.checkType;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PinotSplitManager.class);
    private final String connectorId;
    private final PinotConnection pinotPrestoConnection;
    private final PinotConfig pinotConfig;

    @Inject
    public PinotSplitManager(PinotConnectorId connectorId, PinotConnection pinotPrestoConnection, PinotConfig pinotConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
    }

    private static PushDownLiteral getLiteralFromMarker(Marker marker)
    {
        return getLiteralFromMarkerObject(marker.getType(), marker.getValue());
    }

    private static PushDownLiteral getLiteralFromMarkerObject(Type type, Object value)
    {
        if (value instanceof Slice) {
            Slice slice = (Slice) value;
            return new PushDownLiteral(type.getTypeSignature(), slice.toStringUtf8(), null, null, null);
        }

        if (value instanceof Long) {
            return new PushDownLiteral(type.getTypeSignature(), null, (Long) value, null, null);
        }

        if (value instanceof Double) {
            return new PushDownLiteral(type.getTypeSignature(), null, null, (Double) value, null);
        }

        if (value instanceof Boolean) {
            return new PushDownLiteral(type.getTypeSignature(), null, null, null, (Boolean) value);
        }

        throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unsupported market type in TupleDomain: " + value.getClass());
    }

    static PushDownExpression getPredicate(TupleDomain<ColumnHandle> constraint, Map<ColumnHandle, String> columnAliasMap)
    {
        List<PushDownExpression> expressions = new ArrayList<>();
        Map<ColumnHandle, Domain> columnHandleDomainMap = constraint.getDomains().get();
        for (ColumnHandle k : columnHandleDomainMap.keySet()) {
            Domain domain = columnHandleDomainMap.get(k);
            Optional<PushDownExpression> columnPredicate = getColumnPredicate(domain, ((PinotColumnHandle) k), columnAliasMap.get(k));
            if (columnPredicate.isPresent()) {
                expressions.add(columnPredicate.get());
            }
        }

        Optional<PushDownExpression> predicate = combineExpressions(expressions, "AND");
        if (!predicate.isPresent()) {
            throw new IllegalStateException("TupleDomain resolved to empty predicate: " + constraint);
        }

        return predicate.get();
    }

    static Optional<PushDownExpression> getColumnPredicate(Domain domain, PinotColumnHandle columnHandle, String columnAlias)
    {
        PushDownExpression inputColumn = new PushDownInputColumn(columnHandle.getDataType().getTypeSignature(), columnAlias);
        List<PushDownExpression> conditions = new ArrayList<>();
        TypeSignature booleanType = BOOLEAN.getTypeSignature();
        domain.getValues().getValuesProcessor().consume(
                ranges -> {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            conditions.add(new PushDownLogicalBinaryExpression(booleanType, inputColumn, "=", getLiteralFromMarker(range.getLow())));
                        }
                        else {
                            // get low bound
                            List<PushDownExpression> bounds = new ArrayList<>();
                            if (!range.getLow().isLowerUnbounded()) {
                                String op = (range.getLow().getBound() == Marker.Bound.EXACTLY) ? "<=" : "<";
                                bounds.add(new PushDownLogicalBinaryExpression(booleanType, getLiteralFromMarker(range.getLow()), op, inputColumn));
                            }
                            // get high bound
                            if (!range.getHigh().isUpperUnbounded()) {
                                String op = range.getHigh().getBound() == Marker.Bound.EXACTLY ? "<=" : "<";
                                bounds.add(new PushDownLogicalBinaryExpression(booleanType, inputColumn, op, getLiteralFromMarker(range.getHigh())));
                            }

                            conditions.add(combineExpressions(bounds, "AND").get());
                        }
                    }
                },
                discreteValues -> {
                    if (discreteValues.getValues().isEmpty()) {
                        return;
                    }

                    List<PushDownExpression> inList = discreteValues.getValues().stream().map(v -> getLiteralFromMarkerObject(domain.getValues().getType(), v)).collect(Collectors.toList());
                    conditions.add(new PushDownInExpression(booleanType, discreteValues.isWhiteList(), inputColumn, inList));
                },
                allOrNone ->
                {
                    //no-op
                });

        return combineExpressions(conditions, "OR");
    }

    static Optional<PushDownExpression> combineExpressions(List<PushDownExpression> expressions, String op)
    {
        if (expressions.isEmpty()) {
            return Optional.empty();
        }

        // combine conjucts using the given op
        if (expressions.size() == 1) {
            return Optional.of(expressions.get(0));
        }

        Collections.reverse(expressions);
        PushDownExpression result = expressions.get(0);
        for (int i = 1; i < expressions.size(); i++) {
            result = new PushDownLogicalBinaryExpression(BOOLEAN.getTypeSignature(), expressions.get(i), op, result);
        }

        return Optional.of(result);
    }

    private static TableScanPipeline getScanPipeline(PinotTableLayoutHandle pinotTable)
    {
        if (!pinotTable.getScanPipeline().isPresent()) {
            throw new IllegalArgumentException("Scan pipeline is missing from the Pinot table layout handle");
        }

        return pinotTable.getScanPipeline().get();
    }

    protected static TableScanPipeline addTupleDomainToScanPipelineIfNeeded(Optional<TupleDomain<ColumnHandle>> constraint, TableScanPipeline existingPipeline)
    {
        // if there is no constraint or the constraint selects all then just return the existing pipeline
        if (!constraint.isPresent() || constraint.get().isAll()) {
            return existingPipeline;
        }

        // Check if the pipeline already contains the filter. If it is, then the TupleDomain part is already accommodated into the pipeline
        if (FilterFinder.hasFilter(existingPipeline)) {
            return existingPipeline;
        }

        checkArgument(existingPipeline.getPipelineNodes().size() == 1, "expected to contain just the scan node in pipeline");
        final PipelineNode baseNode = existingPipeline.getPipelineNodes().get(0);
        checkArgument(baseNode instanceof TablePipelineNode, "expected ");
        final TablePipelineNode tablePipelineNode = (TablePipelineNode) baseNode;

        // Create map of underlying column to column alias that TablePipelineNode node exposes as. We are constructing filter on top of the TablePipelineNode and
        // the filter should refer the column names in terms of what is exposed by the TablePipelineNode. TablePipelineNode can map the underlying ColumnHandle to a different
        // name than the actual column name.
        Map<ColumnHandle, String> columnAliasMap = new HashMap<>();
        List<ColumnHandle> inputColumns = tablePipelineNode.getInputColumns();
        List<String> outputColumnAliases = tablePipelineNode.getOutputColumns();
        for (int i = 0; i < inputColumns.size(); i++) {
            columnAliasMap.put(inputColumns.get(i), outputColumnAliases.get(i));
        }

        // convert TupleDomain into FilterPipelineNode
        PushDownExpression predicate = getPredicate(constraint.get(), columnAliasMap);

        final FilterPipelineNode filterNode = new FilterPipelineNode(predicate, tablePipelineNode.getOutputColumns(), tablePipelineNode.getRowType());

        TableScanPipeline newScanPipeline = new TableScanPipeline();
        newScanPipeline.addPipeline(tablePipelineNode, existingPipeline.getOutputColumnHandles());
        newScanPipeline.addPipeline(filterNode, existingPipeline.getOutputColumnHandles());

        return newScanPipeline;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PinotTableLayoutHandle pinotLayoutHandle = checkType(layout, PinotTableLayoutHandle.class, "expected a Pinot table layout handle");

        TableScanPipeline scanPipeline = getScanPipeline(pinotLayoutHandle);

        if (PinotScanParallelismFinder.canParallelize(pinotConfig, scanPipeline)) {
            scanPipeline = addTupleDomainToScanPipelineIfNeeded(pinotLayoutHandle.getConstraint(), scanPipeline);

            return generateSplitsForSegmentBasedScan(pinotLayoutHandle, scanPipeline);
        }
        else {
            return generateSplitForBrokerBasedScan(scanPipeline);
        }
    }

    protected ConnectorSplitSource generateSplitForBrokerBasedScan(TableScanPipeline scanPipeline)
    {
        return new FixedSplitSource(singletonList(createBrokerSplit(connectorId, scanPipeline)));
    }

    protected ConnectorSplitSource generateSplitsForSegmentBasedScan(PinotTableLayoutHandle pinotLayoutHandle, TableScanPipeline scanPipeline)
    {
        PinotTableHandle tableHandle = pinotLayoutHandle.getTable();
        String tableName = tableHandle.getTableName();
        Map<String, Map<String, List<String>>> routingTable;
        Map<String, String> timeBoundary;

        try {
            routingTable = pinotPrestoConnection.getRoutingTable(tableName);
            timeBoundary = pinotPrestoConnection.getTimeBoundary(tableName);
        }
        catch (Exception e) {
            log.error("Failed to fetch table status for Pinot table: %s, Exceptions: %s", tableName, e);
            throw new PinotClientException("Failed to fetch table status for Pinot table: " + tableName, e);
        }

        Optional<String> offlineTimePredicate = Optional.empty();
        Optional<String> onlineTimePredicate = Optional.empty();

        if (timeBoundary.containsKey("timeColumnName") && timeBoundary.containsKey("timeColumnValue")) {
            String timeColumnName = timeBoundary.get("timeColumnName");
            String timeColumnValue = timeBoundary.get("timeColumnValue");

            offlineTimePredicate = Optional.of(format("%s < %s", timeColumnName, timeColumnValue));
            onlineTimePredicate = Optional.of(format("%s >= %s", timeColumnName, timeColumnValue));
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            generateSegmentSplits(splits, routingTable, onlineTimePredicate, tableName, "_REALTIME", scanPipeline);
            generateSegmentSplits(splits, routingTable, offlineTimePredicate, tableName, "_OFFLINE", scanPipeline);
        }

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    protected void generateSegmentSplits(List<ConnectorSplit> splits, Map<String, Map<String, List<String>>> routingTable, Optional<String> timePredicate,
            String tableName, String tableNameSuffix, TableScanPipeline scanPipeline)
    {
        final String finalTableName = tableName + tableNameSuffix;

        for (String routingTableName : routingTable.keySet()) {
            if (!routingTableName.equalsIgnoreCase(finalTableName)) {
                continue;
            }

            String pql = PinotQueryGenerator.generateForSegmentSplits(scanPipeline, Optional.of(tableNameSuffix), timePredicate, Optional.of(pinotConfig)).getPql();

            Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
            for (String host : hostToSegmentsMap.keySet()) {
                for (String segment : hostToSegmentsMap.get(host)) {
                    splits.add(createSegmentSplit(connectorId, pql, segment, host));
                }
            }
        }
    }

    static class FilterFinder
            extends TableScanPipelineVisitor<Boolean, Boolean>
    {
        static boolean hasFilter(TableScanPipeline scanPipeline)
        {
            FilterFinder filterFinder = new FilterFinder();
            for (PipelineNode pipelineNode : scanPipeline.getPipelineNodes()) {
                if (pipelineNode.accept(filterFinder, null)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitNode(PipelineNode node, Boolean context)
        {
            return false;
        }

        @Override
        public Boolean visitFilterNode(FilterPipelineNode filter, Boolean context)
        {
            return true;
        }
    }
}
