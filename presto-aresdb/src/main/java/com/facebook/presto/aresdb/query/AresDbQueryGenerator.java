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
package com.facebook.presto.aresdb.query;

import com.facebook.presto.aresdb.AresDbColumnHandle;
import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.aresdb.query.AresDbExpressionConverter.AresDbExpression;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.pipeline.TableScanPipelineVisitor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.TABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbQueryGenerator
        extends TableScanPipelineVisitor<AresDbQueryGeneratorContext, AresDbQueryGeneratorContext>
{
    private static Map<String, String> unaryAggregationMap = ImmutableMap.of(
            "min", "min",
            "max", "max",
            "avg", "avg",
            "sum", "sum",
            "approx_distinct", "countdistincthll");

    public static AresDbQueryGeneratorContext.AugmentedAQL generate(TableScanPipeline scanPipeline, Optional<List<AresDbColumnHandle>> columnHandles)
    {
        AresDbQueryGeneratorContext context = null;

        AresDbQueryGenerator visitor = new AresDbQueryGenerator();

        for (PipelineNode node : scanPipeline.getPipelineNodes()) {
            context = node.accept(visitor, context);
        }

        return context.toAresDbRequest(columnHandles);
    }

    private static String handleAggregationFunction(AggregationPipelineNode.Aggregation aggregation, Map<String, Selection> inputSelections)
    {
        String prestoAgg = aggregation.getFunction().toLowerCase(ENGLISH);
        List<String> params = aggregation.getInputs();
        switch (prestoAgg) {
            case "count":
                if (params.size() <= 1) {
                    return format("count(%s)", params.isEmpty() ? "*" : inputSelections.get(params.get(0)).getDefinition());
                }
                break;
            default:
                if (unaryAggregationMap.containsKey(prestoAgg) && aggregation.getInputs().size() == 1) {
                    return format("%s(%s)", unaryAggregationMap.get(prestoAgg), inputSelections.get(params.get(0)).getDefinition());
                }
        }

        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, format("aggregation function '%s' not supported yet", aggregation));
    }

    @Override
    public AresDbQueryGeneratorContext visitTableNode(TablePipelineNode table, AresDbQueryGeneratorContext context)
    {
        checkArgument(context == null, "Table scan node is expected to have no context as input");

        AresDbTableHandle tableHandle = (AresDbTableHandle) table.getTableHandle();

        List<ColumnHandle> inputColumns = table.getInputColumns();
        List<String> outputColumns = table.getOutputColumns();

        LinkedHashMap<String, Selection> selections = new LinkedHashMap<>();
        for (int fieldId = 0; fieldId < outputColumns.size(); fieldId++) {
            AresDbColumnHandle aresDbColumn = (AresDbColumnHandle) inputColumns.get(fieldId);
            selections.put(outputColumns.get(fieldId), Selection.of(aresDbColumn.getColumnName(), TABLE, aresDbColumn.getDataType()));
        }

        Optional<String> timeColumn = tableHandle.getTimeColumnName();

        return new AresDbQueryGeneratorContext(selections, tableHandle.getTableName(), timeColumn);
    }

    @Override
    public AresDbQueryGeneratorContext visitProjectNode(ProjectPipelineNode project, AresDbQueryGeneratorContext context)
    {
        requireNonNull(context, "context is null");

        LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();

        List<PushDownExpression> pushdownExpressions = project.getExprs();
        List<String> outputColumns = project.getOutputColumns();
        List<Type> outputTypes = project.getRowType();
        for (int fieldId = 0; fieldId < pushdownExpressions.size(); fieldId++) {
            PushDownExpression pushdownExpression = pushdownExpressions.get(fieldId);
            AresDbExpression aresDbExpression = pushdownExpression.accept(new AresDbExpressionConverter(), context.getSelections());
            newSelections.put(
                    outputColumns.get(fieldId),
                    Selection.of(aresDbExpression.getDefinition(), aresDbExpression.getOrigin(), outputTypes.get(fieldId), aresDbExpression.getTimeBucketizer()));
        }

        return context.withProject(newSelections);
    }

    private static boolean isBooleanTrue(PushDownExpression expression)
    {
        if (!(expression instanceof PushDownLiteral)) {
            return false;
        }
        Boolean booleanValue = ((PushDownLiteral) expression).getBooleanValue();
        return booleanValue != null && booleanValue.booleanValue();
    }

    @Override
    public AresDbQueryGeneratorContext visitFilterNode(FilterPipelineNode filter, AresDbQueryGeneratorContext context)
    {
        requireNonNull(context, "context is null");
        Optional<Domain> timeFilter;
        PushDownExpression predicate = filter.getPredicate();
        Optional<String> filterPredicateOptional;
        if (isBooleanTrue(predicate)) {
            filterPredicateOptional = Optional.empty();
        }
        else {
            String filterPredicate = predicate.accept(new AresDbExpressionConverter(), context.getSelections()).getDefinition();
            if (filterPredicate == null) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Cannot convert the filter %s", predicate));
            }
            filterPredicateOptional = Optional.of(filterPredicate);
        }
        if (context.getTimeColumn().isPresent() && filter.getSymbolNameToDomains().isPresent()) {
            String timeColumn = context.getTimeColumn().get().toLowerCase(ENGLISH);
            timeFilter = filter.getSymbolNameToDomains().get().getDomains().flatMap(domains -> Optional.ofNullable(domains.get(timeColumn)));
        }
        else {
            timeFilter = Optional.empty();
        }
        return context.withFilter(filterPredicateOptional, timeFilter);
    }

    @Override
    public AresDbQueryGeneratorContext visitAggregationNode(AggregationPipelineNode aggregation, AresDbQueryGeneratorContext context)
    {
        requireNonNull(context, "context is null");
        checkArgument(!aggregation.isPartial(), "partial aggregations are not supported in AresDb pushdown framework");

        LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
        List<String> groupByColumns = new ArrayList<>();

        boolean groupByExists = false;
        int numberOfAggregations = 0;
        for (AggregationPipelineNode.Node expr : aggregation.getNodes()) {
            switch (expr.getExprType()) {
                case GROUP_BY: {
                    AggregationPipelineNode.GroupByColumn groupByColumn = (AggregationPipelineNode.GroupByColumn) expr;
                    String groupByInputColumn = groupByColumn.getInputColumn();
                    Selection aresDbColumn = requireNonNull(context.getSelections().get(groupByInputColumn), "Group By column doesn't exist in input");

                    groupByColumns.add(groupByColumn.getOutputColumn());
                    newSelections.put(groupByColumn.getOutputColumn(),
                            Selection.of(aresDbColumn.getDefinition(), aresDbColumn.getOrigin(), groupByColumn.getOutputType(), aresDbColumn.getTimeTokenizer()));
                    groupByExists = true;
                    break;
                }
                case AGGREGATE: {
                    AggregationPipelineNode.Aggregation aggr = (AggregationPipelineNode.Aggregation) expr;
                    String aresDbAggFunction = handleAggregationFunction(aggr, context.getSelections());
                    newSelections.put(aggr.getOutputColumn(), Selection.of(aresDbAggFunction, DERIVED, aggr.getOutputType()));
                    numberOfAggregations++;
                    break;
                }
                default:
                    throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "unknown aggregation expression: " + expr.getExprType());
            }
        }

        if (numberOfAggregations > 1 && groupByExists) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "AresDB doesn't support semantically SQL equivalent multiple aggregations with GROUP BY in a query");
        }

        return context.withAggregation(newSelections, groupByColumns);
    }

    @Override
    public AresDbQueryGeneratorContext visitLimitNode(LimitPipelineNode limit, AresDbQueryGeneratorContext context)
    {
        requireNonNull(context, "context is null");
        return context.withLimit(limit.getLimit());
    }
}
