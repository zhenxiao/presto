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
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.ScanNode.pipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Rule that generates a new table layout with scan pipeline pushed into the connector table layout
 *
 * <p>
 * From:
 * <pre>
 * - TableScan
 *   - ScanPipeline
 *   - TableLayout
 *     - ConnectorTableLayout
 * </pre>
 * To:
 * <pre>
 * - TableScan
 *   - TableLayout
 *     - ConnectorTableLayout
 *       - ScanPipeline
 * </pre>
 * <p>
 */
public class GenerateTableLayoutWithPipeline
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan()
            .with(pipeline().matching(t -> t != null));

    private final Metadata metadata;

    public GenerateTableLayoutWithPipeline(Metadata metadata)
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
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();

        Optional<TableLayoutHandle> newLayoutHandle = metadata.pushTableScanIntoConnectorTableLayout(
                context.getSession(), scanNode.getLayout().get(), scanNode.getScanPipeline().get());

        if (newLayoutHandle.isPresent()) {
            return Result.ofPlanNode(new TableScanNode(
                    idAllocator.getNextId(),
                    scanNode.getTable(),
                    scanNode.getOutputSymbols(),
                    scanNode.getAssignments(),
                    newLayoutHandle,
                    scanNode.getCurrentConstraint(),
                    scanNode.getEnforcedConstraint(),
                    Optional.empty()));
        }

        throw new RuntimeException("shouldn't happen"); // TODO: handle it in a better way
    }
}
