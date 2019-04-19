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
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.iterative.rule.PushDownUtils.newTableScanWithPipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.Limit.partial;
import static com.facebook.presto.sql.planner.plan.Patterns.ScanNode.hasPipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public abstract class PushLimitIntoTableScan
        implements Rule<LimitNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<LimitNode> PARTIAL_LIMIT_PATTERN = limit()
            .with(partial().equalTo(true))
            .with(source().matching(tableScan().with(hasPipeline().matching(t -> t)).capturedAs(TABLE_SCAN)));

    private static final Pattern<LimitNode> FINAL_LIMIT_PATTERN = limit()
            .with(partial().equalTo(false))
            .with(source().matching(tableScan().with(hasPipeline().matching(t -> t)).capturedAs(TABLE_SCAN)));

    private final Metadata metadata;
    private final Pattern<LimitNode> pattern;
    private final boolean partial;

    public PushLimitIntoTableScan(Metadata metadata, Pattern<LimitNode> pattern, boolean partial)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.partial = partial;
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Rule.Result apply(LimitNode limitNode, Captures captures, Rule.Context context)
    {
        TableScanNode scanNode = captures.get(TABLE_SCAN);
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();

        LimitPipelineNode limitPipelineNode = new LimitPipelineNode(
                limitNode.getCount(),
                partial,
                limitNode.getOutputSymbols().stream().map(s -> s.getName()).collect(Collectors.toList()),
                limitNode.getOutputSymbols().stream().map(s -> context.getSymbolAllocator().getTypes().get(s)).collect(Collectors.toList()));

        Optional<TableScanPipeline> newScanPipeline = metadata.pushLimitIntoScan(
                context.getSession(), scanNode.getTable(), scanNode.getOrCreateScanPipeline(typeProvider), limitPipelineNode);

        if (newScanPipeline.isPresent()) {
            return Rule.Result.ofPlanNode(newTableScanWithPipeline(scanNode, context.getIdAllocator().getNextId(), limitNode.getOutputSymbols(), newScanPipeline.get()));
        }

        return Rule.Result.empty();
    }

    /**
     * Pushes the final limit above the table scan into table scan.
     *
     * <p>
     * From:
     * <pre>
     * - Limit (Partial = false)
     *   - TableScan
     * </pre>
     * To:
     * <pre>
     * - TableScan (with limit pushed into the scan)
     * </pre>
     * <p>
     */
    public static class FinalLimitPushDown
            extends PushLimitIntoTableScan
    {
        public FinalLimitPushDown(Metadata metadata)
        {
            super(metadata, FINAL_LIMIT_PATTERN, false);
        }
    }

    /**
     * Pushes limit into table scan.
     *
     * <p>
     * From:
     * <pre>
     *   - Limit (Partial = true)
     *     - TableScan
     * </pre>
     * To:
     * <pre>
     * - TableScan (with limit pushed into the scan)
     * </pre>
     * <p>
     */
    public static class PartialLimitPushDown
            extends PushLimitIntoTableScan
    {
        public PartialLimitPushDown(Metadata metadata)
        {
            super(metadata, PARTIAL_LIMIT_PATTERN, true);
        }
    }
}
