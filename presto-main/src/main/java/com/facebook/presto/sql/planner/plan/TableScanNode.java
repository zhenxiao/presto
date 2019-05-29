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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final TableHandle table;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column

    private final Optional<TableLayoutHandle> tableLayout;
    private final Optional<TableScanPipeline> scanPipeline;

    // Used during predicate refinement over multiple passes of predicate pushdown
    // TODO: think about how to get rid of this in new planner
    private final TupleDomain<ColumnHandle> currentConstraint;

    private final TupleDomain<ColumnHandle> enforcedConstraint;

    private final TupleDomain<? extends ColumnHandle> compactEffectiveConstraint;

    @JsonCreator
    public TableScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputSymbols") List<Symbol> outputs,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("layout") Optional<TableLayoutHandle> tableLayout,
            @JsonProperty("pipeline") Optional<TableScanPipeline> scanPipeline)
    {
        // This constructor is for JSON deserialization only. Do not use.
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");
        this.tableLayout = requireNonNull(tableLayout, "tableLayout is null");
        this.currentConstraint = null;
        this.enforcedConstraint = null;
        this.scanPipeline = scanPipeline;
        this.compactEffectiveConstraint = null;
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments)
    {
        this(id, table, outputs, assignments, Optional.empty(), TupleDomain.all(), TupleDomain.all(), Optional.empty());
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            Optional<TableLayoutHandle> tableLayout,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<TableScanPipeline> scanPipeline)
    {
        this(id, table, outputs, assignments, tableLayout, currentConstraint, enforcedConstraint, TupleDomain.all(), scanPipeline);
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            Optional<TableLayoutHandle> tableLayout,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint,
            TupleDomain<? extends ColumnHandle> compactEffectiveConstraint,
            Optional<TableScanPipeline> scanPipeline)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");
        this.tableLayout = requireNonNull(tableLayout, "tableLayout is null");
        this.currentConstraint = requireNonNull(currentConstraint, "currentConstraint is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        if (!currentConstraint.isAll() || !enforcedConstraint.isAll()) {
            checkArgument(tableLayout.isPresent(), "tableLayout must be present when currentConstraint or enforcedConstraint is non-trivial");
        }
        if (scanPipeline.isPresent()) {
            ImmutableSet<String> columnsInLastPipelineNode = ImmutableSet.copyOf(scanPipeline.get().getLastPipelineNodeOutputColumns());
            ImmutableSet<String> newSymbols = assignments.keySet().stream().map(Symbol::getName).collect(toImmutableSet());
            checkState(newSymbols.equals(columnsInLastPipelineNode), "Mismatch of columns %s in scan pipeline %s vs new output symbols %s", columnsInLastPipelineNode, scanPipeline.get(), newSymbols);
        }
        this.scanPipeline = scanPipeline;
        this.compactEffectiveConstraint = requireNonNull(compactEffectiveConstraint, "compactEffectiveConstraint is null");
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<TableLayoutHandle> getLayout()
    {
        return tableLayout;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    /**
     * A TupleDomain that represents a predicate that every row this TableScan node
     * produces is guaranteed to satisfy.
     * <p>
     * This guarantee can have different origins.
     * For example, it may be successful predicate push down, or inherent guarantee provided by the underlying data.
     */
    public TupleDomain<ColumnHandle> getCurrentConstraint()
    {
        // currentConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(currentConstraint != null, "currentConstraint should only be used in planner. It is not transported to workers.");
        return currentConstraint;
    }

    /**
     * A TupleDomain that represents a predicate that has been successfully pushed into
     * this TableScan node. In other words, predicates that were removed from filters
     * above the TableScan node because the TableScan node can guarantee it.
     * <p>
     * This field is used to make sure that predicates which were previously pushed down
     * do not get lost in subsequent refinements of the table layout.
     */
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        // enforcedConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(enforcedConstraint != null, "enforcedConstraint should only be used in planner. It is not transported to workers.");
        return enforcedConstraint;
    }

    @JsonProperty("pipeline")
    public Optional<TableScanPipeline> getScanPipeline()
    {
        return scanPipeline;
    }

    public TableScanPipeline getOrCreateScanPipeline(TypeProvider typeProvider)
    {
        if (scanPipeline.isPresent()) {
            return scanPipeline.get();
        }

        // Create a base pipeline with base scan
        TablePipelineNode tablePipelineNode = new TablePipelineNode(
                table.getConnectorHandle(),
                outputSymbols.stream().map(s -> assignments.get(s)).collect(Collectors.toList()),
                outputSymbols.stream().map(s -> s.getName()).collect(Collectors.toList()),
                outputSymbols.stream().map(s -> typeProvider.get(s)).collect(Collectors.toList()));

        TableScanPipeline newScanPipeline = new TableScanPipeline();
        newScanPipeline.addPipeline(tablePipelineNode, outputSymbols.stream().map(s -> assignments.get(s)).collect(Collectors.toList()));

        return newScanPipeline;
    }

    public TupleDomain<? extends ColumnHandle> getCompactEffectiveConstraint()
    {
        checkState(compactEffectiveConstraint != null, "compactEffectiveConstraint should only be used in planner. It is not transported to workers.");
        return compactEffectiveConstraint;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tableLayout", tableLayout)
                .add("outputSymbols", outputSymbols)
                .add("assignments", assignments)
                .add("currentConstraint", currentConstraint)
                .add("enforcedConstraint", enforcedConstraint)
                .toString();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
