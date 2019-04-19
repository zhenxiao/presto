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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.iterative.rule.PushDownUtils.newTableScanWithPipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.ScanNode.hasPipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Create {@link TableScanPipeline} for {@link TableScanNode}. Scan pipeline helps pushing further operations into the table scan.
 *
 * <p>
 * From:
 * <pre>
 * - TableScan
 * </pre>
 * To:
 * <pre>
 * - TableScan (with TableScanPipeline)
 * </pre>
 * <p>
 */
public class CreateTableScanPipeline
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan().with(hasPipeline().matching(t -> !t));

    private final Metadata metadata;

    public CreateTableScanPipeline(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode scanNode, Captures captures, Context context)
    {
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();

        TablePipelineNode tablePipelineNode = new TablePipelineNode(
                scanNode.getTable().getConnectorHandle(),
                scanNode.getOutputSymbols().stream()
                        .map(s -> scanNode.getAssignments().get(s))
                        .collect(Collectors.toList()),
                scanNode.getOutputSymbols().stream().map(s -> s.getName()).collect(Collectors.toList()),
                scanNode.getOutputSymbols().stream().map(s -> typeProvider.get(s)).collect(Collectors.toList()));

        Optional<TableScanPipeline> newScanPipeline = metadata.convertToTableScanPipeline(
                context.getSession(), scanNode.getTable(), tablePipelineNode);

        if (newScanPipeline.isPresent()) {
            return Result.ofPlanNode(newTableScanWithPipeline(scanNode, context.getIdAllocator().getNextId(), scanNode.getOutputSymbols(), newScanPipeline.get()));
        }

        return Result.empty();
    }
}
