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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.extractPredicates;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.AND;
import static java.util.Objects.requireNonNull;

public class AggregationPushDown
        implements PlanOptimizer
{
    public static final Set<String> PUSHDOWN_AGGREGATIONS = ImmutableSet.of("count", "max", "min");

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!SystemSessionProperties.isAggregationPushDown(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new HashMap<String, List<String>>());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Map<String, List<String>>>
    {
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<String, List<String>>> context)
        {
            int numberOfCount = 0;
            Map<String, List<String>> aggregations = context.get();
            for (List<String> functions : aggregations.values()) {
                if (functions.contains("count")) {
                    if (numberOfCount >= 1) {
                        // Support just one count aggregation
                        return node;
                    }
                    numberOfCount = numberOfCount + 1;
                }
            }
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint(),
                    node.getNestedFields(),
                    node.getJsonPaths(),
                    node.getLimit(),
                    Optional.of(aggregations));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Map<String, List<String>>> context)
        {
            List<Expression> expressions = new ArrayList<>();
            expressions.addAll(extractPredicates(AND, node.getPredicate()));
            Iterator<Expression> it = expressions.iterator();
            while (it.hasNext()) {
                if (processNullFilter(context.get(), it.next())) {
                    it.remove();
                }
            }
            for (String symbol : extractSymbols(expressions)) {
                addAggregations(context.get(), symbol, null);
            }
            PlanNode source = context.rewrite(node.getSource(), context.get());
            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Map<String, List<String>>> context)
        {
            if (!(node.getGroupingKeys().isEmpty() && isProjectFilterTableScan(node.getSource()))) {
                return node;
            }

            Map<String, List<String>> aggregations = context.get();
            for (Aggregation aggregation : node.getAggregations().values()) {
                String functionName = aggregation.getCall().getName().toString();
                List<Expression> args = aggregation.getCall().getArguments();
                if (functionName.equals("count") && args.isEmpty()) {
                    addAggregations(aggregations, "*", functionName);
                    continue;
                }
                if (args.size() != 1 || !PUSHDOWN_AGGREGATIONS.contains(functionName)) {
                    return node;
                }
                Expression expression = args.get(0);
                if (!(expression instanceof SymbolReference)) {
                    return node;
                }
                addAggregations(aggregations, ((SymbolReference) expression).getName(), functionName);
            }
            PlanNode source = context.rewrite(node.getSource(), aggregations);
            return new AggregationNode(
                    node.getId(),
                    source,
                    node.getAggregations(),
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        private static void addAggregations(Map<String, List<String>> aggregations, String columnName, String functionName)
        {
            List<String> functionNames = aggregations.get(columnName);
            if (functionNames == null) {
                functionNames = new ArrayList<>();
                aggregations.put(columnName, functionNames);
            }
            if (!functionNames.contains(functionName)) {
                functionNames.add(functionName);
            }
        }

        private static boolean isProjectFilterTableScan(PlanNode node)
        {
            if (node instanceof ProjectNode) {
                return isProjectFilterTableScan(((ProjectNode) node).getSource());
            }
            if (node instanceof FilterNode) {
                return isProjectFilterTableScan(((FilterNode) node).getSource());
            }
            return node instanceof TableScanNode;
        }

        private static boolean processNullFilter(Map<String, List<String>> aggregations, Expression predicate)
        {
            Expression expression = null;
            boolean isNull = true;
            if (predicate instanceof NotExpression) {
                isNull = false;
                predicate = ((NotExpression) predicate).getValue();
            }
            if (predicate instanceof IsNullPredicate) {
                expression = ((IsNullPredicate) predicate).getValue();
            }
            else if (predicate instanceof IsNotNullPredicate) {
                expression = ((IsNotNullPredicate) predicate).getValue();
                isNull = isNull ^ false;
            }

            if (expression == null || !(expression instanceof SymbolReference)) {
                return false;
            }
            addAggregations(aggregations, ((SymbolReference) expression).getName(), isNull ? "isnull" : "notnull");
            return true;
        }

        private static Set<String> extractSymbols(List<Expression> nodes)
        {
            DefaultExpressionTraversalVisitor<Void, Set<String>> visitor = new DefaultExpressionTraversalVisitor<Void, Set<String>>() {
                @Override
                protected Void visitSymbolReference(SymbolReference node, Set<String> context)
                {
                    context.add(node.getName());
                    return null;
                }
            };

            Set<String> symbols = new HashSet<>();
            for (Expression node : nodes) {
                visitor.process(node, symbols);
            }
            return symbols;
        }
    }
}
