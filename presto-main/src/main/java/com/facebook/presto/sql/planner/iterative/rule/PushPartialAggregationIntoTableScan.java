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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.sql.planner.iterative.rule.PushDownUtils.convertAggregationToPushDownFormat;
import static com.facebook.presto.sql.planner.iterative.rule.PushDownUtils.newTableScanWithPipeline;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.ScanNode.hasPipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Pushes partial aggregation into table scan. Useful in connectors which can compute faster than Presto or
 * already have pre-computed partial un-grouped aggregations by split level
 *
 * <p>
 * From:
 * <pre>
 * - Aggregation (PARTIAL)
 *   - TableScan
 * </pre>
 * To:
 * <pre>
 * - TableScan (with aggregation pushed into the scan)
 * </pre>
 * <p>
 */
public class PushPartialAggregationIntoTableScan
        implements Rule<AggregationNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            // Only consider PARTIAL un-grouped aggregations
            .with(step().equalTo(PARTIAL))
            .with(empty(groupingColumns()))
            // Only consider aggregations without ORDER BY clause
            .matching(node -> !node.hasOrderings())
            .with(source().matching(tableScan().with(hasPipeline().matching(t -> t)).capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushPartialAggregationIntoTableScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        TableScanNode scanNode = captures.get(TABLE_SCAN);
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();

        Optional<AggregationPipelineNode> aggPipelineNode = convertAggregationToPushDownFormat(
                true,
                aggregation.getOutputSymbols(),
                aggregation.getAggregations(),
                aggregation.getGroupingKeys(),
                aggregation.getOutputSymbols(),
                context.getSymbolAllocator().getTypes());

        if (!aggPipelineNode.isPresent()) {
            return Result.empty();
        }

        Optional<TableScanPipeline> newScanPipeline = metadata.pushAggregationIntoScan(
                context.getSession(), scanNode.getTable(), scanNode.getOrCreateScanPipeline(typeProvider), aggPipelineNode.get());

        if (newScanPipeline.isPresent()) {
            return Result.ofPlanNode(newTableScanWithPipeline(scanNode, context.getIdAllocator().getNextId(), aggregation.getOutputSymbols(), newScanPipeline.get()));
        }

        return Result.empty();
    }
}
