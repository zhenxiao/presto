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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.query.PinotExpressionConverter.PinotExpression;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Aggregation;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.SortPipelineNode;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.pipeline.TableScanPipelineVisitor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.LITERAL;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotQueryGenerator
{
    private PinotQueryGenerator()
    {
    }

    public static GeneratedPql generate(TableScanPipeline scanPipeline, Optional<List<PinotColumnHandle>> columnHandles, Optional<String> tableNameSuffix,
            Optional<String> timeBoundaryFilter, Optional<PinotConfig> pinotConfig)
    {
        PinotQueryGeneratorContext context = null;

        PinotPushDownPipelineConverter visitor = new PinotPushDownPipelineConverter(tableNameSuffix, timeBoundaryFilter);

        for (PipelineNode node : scanPipeline.getPipelineNodes()) {
            context = node.accept(visitor, context);
        }

        return context.toQuery(columnHandles, pinotConfig);
    }

    public static String generatePQL(TableScanPipeline scanPipeline)
    {
        return generate(scanPipeline, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()).getPql();
    }

    public static class GeneratedPql
    {
        final String pql;
        final Optional<List<Integer>> columnIndicesExpected;
        final int numGroupByClauses;

        public GeneratedPql(String pql, Optional<List<Integer>> columnIndicesExpected, int numGroupByClauses)
        {
            this.pql = pql;
            this.columnIndicesExpected = columnIndicesExpected;
            this.numGroupByClauses = numGroupByClauses;
        }

        public String getPql()
        {
            return pql;
        }

        public Optional<List<Integer>> getColumnIndicesExpected()
        {
            return columnIndicesExpected;
        }

        public int getNumGroupByClauses()
        {
            return numGroupByClauses;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pql", pql)
                    .add("columnIndicesExpected", columnIndicesExpected)
                    .add("numGroupByClauses", numGroupByClauses)
                    .toString();
        }
    }

    /**
     * Convert {@link TableScanPipeline} into {@link PinotQueryGeneratorContext}
     */
    static class PinotPushDownPipelineConverter
            extends TableScanPipelineVisitor<PinotQueryGeneratorContext, PinotQueryGeneratorContext>
    {
        private static Map<String, String> unaryAggregationMap = ImmutableMap.of(
                "min", "min",
                "max", "max",
                "avg", "avg",
                "sum", "sum",
                "approx_distinct", "DISTINCTCOUNTHLL");

        private final Optional<String> tableNameSuffix;
        private final Optional<String> timeBoundaryFilter;

        public PinotPushDownPipelineConverter(Optional<String> tableNameSuffix, Optional<String> timeBoundaryFilter)
        {
            this.tableNameSuffix = tableNameSuffix;
            this.timeBoundaryFilter = timeBoundaryFilter;
        }

        private static String handleAggregationFunction(Aggregation aggregation, Map<String, Selection> inputSelections)
        {
            String prestoAgg = aggregation.getFunction().toLowerCase(ENGLISH);
            List<String> params = aggregation.getInputs();
            switch (prestoAgg) {
                case "count":
                    if (params.size() <= 1) {
                        return format("count(%s)", params.isEmpty() ? "*" : inputSelections.get(params.get(0)));
                    }
                    break;
                case "approx_percentile":
                    return handleApproxPercentile(aggregation, inputSelections);
                default:
                    if (unaryAggregationMap.containsKey(prestoAgg) && aggregation.getInputs().size() == 1) {
                        return format("%s(%s)", unaryAggregationMap.get(prestoAgg), inputSelections.get(params.get(0)));
                    }
            }

            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("aggregation function '%s' not supported yet", aggregation));
        }

        private static String handleApproxPercentile(Aggregation aggregation, Map<String, Selection> inputSelections)
        {
            List<String> inputs = aggregation.getInputs();
            if (inputs.size() != 2) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Cannot handle approx_percentile function " + aggregation);
            }

            Selection percentage = inputSelections.get(inputs.get(1));
            int percentile;
            if (percentage.getOrigin() != LITERAL) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        "Cannot handle approx_percentile percentage argument be a non literal " + aggregation);
            }

            percentile = getValidPercentile(percentage.getDefinition());
            if (percentile < 0 || percentile > 100) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        format("Cannot handle approx_percentile parsed as %d from function %s", percentile, aggregation));
            }
            return format("PERCENTILEEST%d(%s)", percentile, inputSelections.get(inputs.get(0)));
        }

        private static int getValidPercentile(String percentage)
        {
            try {
                int percentile = Integer.parseInt(percentage, 10);
                if (percentile >= 0 && percentile <= 100) {
                    return percentile;
                }
            }
            catch (NumberFormatException ne) {
                // Skip
            }
            try {
                double percentileDouble = Double.parseDouble(percentage);
                if (percentileDouble >= 0 && percentileDouble <= 100
                        && percentileDouble == Math.floor(percentileDouble) && !Double.isInfinite(percentileDouble)) {
                    return (int) percentileDouble;
                }
            }
            catch (NumberFormatException ne) {
                // Skip
            }
            return -1;
        }

        @Override
        public PinotQueryGeneratorContext visitAggregationNode(AggregationPipelineNode aggregation, PinotQueryGeneratorContext context)
        {
            requireNonNull(context, "context is null");
            checkArgument(!aggregation.isPartial(), "partial aggregations are not supported in Pinot pushdown framework");

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
            List<String> groupByColumns = new ArrayList<>();

            boolean groupByExists = false;
            int numberOfAggregations = 0;
            for (AggregationPipelineNode.Node expr : aggregation.getNodes()) {
                switch (expr.getExprType()) {
                    case GROUP_BY: {
                        AggregationPipelineNode.GroupByColumn groupByColumn = (AggregationPipelineNode.GroupByColumn) expr;
                        String groupByInputColumn = groupByColumn.getInputColumn();
                        Selection pinotColumn = requireNonNull(context.getSelections().get(groupByInputColumn), "Group By column doesn't exist in input");

                        groupByColumns.add(pinotColumn.getDefinition());
                        newSelections.put(groupByColumn.getOutputColumn(), new Selection(pinotColumn.getDefinition(), pinotColumn.getOrigin(), groupByColumn.getOutputType()));
                        groupByExists = true;
                        break;
                    }
                    case AGGREGATE: {
                        Aggregation aggr = (Aggregation) expr;
                        String pinotAggFunction = handleAggregationFunction(aggr, context.getSelections());
                        newSelections.put(aggr.getOutputColumn(), new Selection(pinotAggFunction, DERIVED, aggr.getOutputType()));
                        numberOfAggregations++;
                        break;
                    }
                    default:
                        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unknown aggregation expression: " + expr.getExprType());
                }
            }

            if (numberOfAggregations > 1 && groupByExists) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot doesn't support semantically SQL equivalent multiple aggregations with GROUP BY in a query");
            }

            return context.withAggregation(newSelections, groupByColumns);
        }

        @Override
        public PinotQueryGeneratorContext visitFilterNode(FilterPipelineNode filter, PinotQueryGeneratorContext context)
        {
            requireNonNull(context, "context is null");
            String predicate = filter.getPredicate().accept(new PinotExpressionConverter(), context.getSelections()).getDefinition();
            return context.withFilter(predicate);
        }

        @Override
        public PinotQueryGeneratorContext visitProjectNode(ProjectPipelineNode project, PinotQueryGeneratorContext context)
        {
            requireNonNull(context, "context is null");

            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();

            List<PushDownExpression> pushdownExpressions = project.getExprs();
            List<String> outputColumns = project.getOutputColumns();
            List<Type> outputTypes = project.getRowType();
            for (int fieldId = 0; fieldId < pushdownExpressions.size(); fieldId++) {
                PushDownExpression pushdownExpression = pushdownExpressions.get(fieldId);
                PinotExpression pinotExpression = pushdownExpression.accept(new PinotExpressionConverter(), context.getSelections());
                newSelections.put(
                        outputColumns.get(fieldId),
                        new Selection(pinotExpression.getDefinition(), pinotExpression.getOrigin(), outputTypes.get(fieldId)));
            }

            return context.withProject(newSelections);
        }

        @Override
        public PinotQueryGeneratorContext visitLimitNode(LimitPipelineNode limit, PinotQueryGeneratorContext context)
        {
            requireNonNull(context, "context is null");
            return context.withLimit(limit.getLimit());
        }

        @Override
        public PinotQueryGeneratorContext visitSortNode(SortPipelineNode limit, PinotQueryGeneratorContext context)
        {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Sort is not supported yet");
        }

        @Override
        public PinotQueryGeneratorContext visitTableNode(TablePipelineNode table, PinotQueryGeneratorContext context)
        {
            checkArgument(context == null, "Table scan node is expected to have no context as input");

            PinotTableHandle tableHandle = (PinotTableHandle) table.getTableHandle();

            List<ColumnHandle> inputColumns = table.getInputColumns();
            List<String> outputColumns = table.getOutputColumns();

            LinkedHashMap<String, Selection> selections = new LinkedHashMap<>();
            for (int fieldId = 0; fieldId < outputColumns.size(); fieldId++) {
                PinotColumnHandle pinotColumn = (PinotColumnHandle) inputColumns.get(fieldId);
                selections.put(outputColumns.get(fieldId), new Selection(pinotColumn.getColumnName(), TABLE_COLUMN, pinotColumn.getDataType()));
            }

            return new PinotQueryGeneratorContext(selections, tableHandle.getTableName() + tableNameSuffix.orElse(""), timeBoundaryFilter);
        }
    }
}
