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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.pipeline.JoinPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PushDownExpressionGenerator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.rule.PushDownUtils.newTableScanWithPipeline;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.collect.ImmutableList.toImmutableList;

// We will only push the right side into the left's pipeline for now.
// TODO: we can handle the other case by adding a project etc
public class PushJoinIntoTableScan
        implements Rule<JoinNode>
{
    // TODO: I can't find good examples of how to break past the GroupedReference without putting the rule inline in the apply
    private static final Pattern<JoinNode> PATTERN = join();

    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushJoinIntoTableScan(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = metadata;
        this.sqlParser = sqlParser;
    }

    private static Optional<TableScanNode> getScanNode(PlanNode node, Lookup lookup)
    {
        node = lookup.resolve(node);
        if ((node instanceof TableScanNode) && (((TableScanNode) node).getScanPipeline().isPresent())) {
            return Optional.of((TableScanNode) node);
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode join, Captures captures, Context context)
    {
        Optional<TableScanNode> left = getScanNode(join.getLeft(), context.getLookup());
        Optional<TableScanNode> right = getScanNode(join.getRight(), context.getLookup());

        if (!left.isPresent() || !right.isPresent() || !left.get().getScanPipeline().isPresent() || !right.get().getScanPipeline().isPresent()) {
            return Result.empty();
        }
        ConnectorId leftConnectorId = left.get().getTable().getConnectorId();
        ConnectorId rightConnectorId = right.get().getTable().getConnectorId();
        if (leftConnectorId == null || rightConnectorId == null || !leftConnectorId.equals(rightConnectorId)) {
            return Result.empty();
        }

        List<Symbol> originalDesiredOutputSymbols = join.getOutputSymbols();

        Optional<JoinPipelineNode> joinPipelineNode = inConnectorFormat(join, context, left.get(), right.get());

        if (!joinPipelineNode.isPresent()) {
            return Result.empty();
        }

        Optional<TableScanPipeline> newScanPipeline = metadata.pushRightJoinIntoScan(
                context.getSession(), left.get().getTable(), left.get().getScanPipeline().get(), joinPipelineNode.get());

        if (newScanPipeline.isPresent()) {
            PlanNodeIdAllocator idAllocator = context.getIdAllocator();
            TableScanNode newScanNode = newTableScanWithPipeline(left.get(), idAllocator.getNextId(), join.getOutputSymbols(), newScanPipeline.get());
            return Result.ofPlanNode(newScanNode);
        }

        return Result.empty();
    }

    private static JoinPipelineNode.JoinType convertJoinType(JoinNode.Type joinType)
    {
        switch (joinType) {
            case INNER:
                return JoinPipelineNode.JoinType.INNER;
            case LEFT:
                return JoinPipelineNode.JoinType.LEFT;
            case RIGHT:
                return JoinPipelineNode.JoinType.RIGHT;
            case FULL:
                return JoinPipelineNode.JoinType.FULL;
            default:
                throw new UnsupportedOperationException("Cannot deal with join type " + joinType);
        }
    }

    Optional<JoinPipelineNode> inConnectorFormat(JoinNode joinNode, Context context, TableScanNode left, TableScanNode right)
    {
        PushDownUtils.ExpressionToTypeConverter typeConverter = new PushDownUtils.ExpressionToTypeConverterImpl(context, metadata, sqlParser);
        Optional<PushDownExpression> filterExpression;
        if (joinNode.getFilter().isPresent()) {
            filterExpression = Optional.ofNullable(new PushDownExpressionGenerator(typeConverter).process(joinNode.getFilter().get()));
            if (!filterExpression.isPresent()) {
                return Optional.empty();
            }
        }
        else {
            filterExpression = Optional.empty();
        }

        JoinNode.Type joinNodeType = joinNode.getType();
        List<String> outputSymbols = joinNode.getOutputSymbols().stream().map(s -> s.getName()).collect(toImmutableList());
        List<Type> rowType = joinNode.getOutputSymbols().stream().map(s -> context.getSymbolAllocator().getTypes().get(s)).collect(toImmutableList());
        ImmutableList.Builder<JoinPipelineNode.EquiJoinClause> equiJoinClauseBuilder = ImmutableList.builder();
        for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
            Optional<PushDownExpression> leftPD = Optional.ofNullable(new PushDownExpressionGenerator(typeConverter).process(new SymbolReference(equiJoinClause.getLeft().getName())));
            Optional<PushDownExpression> rightPD = Optional.ofNullable(new PushDownExpressionGenerator(typeConverter).process(new SymbolReference(equiJoinClause.getRight().getName())));
            if (!leftPD.isPresent() || !rightPD.isPresent()) {
                return Optional.empty();
            }
            equiJoinClauseBuilder.add(new JoinPipelineNode.EquiJoinClause(leftPD.get(), rightPD.get()));
        }
        return Optional.of(new JoinPipelineNode(filterExpression, outputSymbols, rowType, right.getTable().getConnectorHandle(), right.getScanPipeline(), equiJoinClauseBuilder.build(), convertJoinType(joinNodeType)));
    }
}
