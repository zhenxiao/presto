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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JsonPathPushdown
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!SystemSessionProperties.isJsonPathPushDown(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new HashMap<>());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Map<String, String>>
    {
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<String, String>> context)
        {
            if (node.getOriginalConstraint() == null) {
                return node;
            }
            Map<String, String> functions = context.get();
            process(node.getOriginalConstraint(), functions);
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint(),
                    node.getNestedFields(),
                    Optional.of(functions),
                    node.getLimit(),
                    node.getAggregations());
        }

        private static void process(Expression expression, Map<String, String> functions)
        {
            new JsonFunctionVisitor().process(expression, new JsonFunctionContext(functions));
        }

        private static class JsonFunctionContext
        {
            private Map<String, String> functions;

            public JsonFunctionContext(Map<String, String> functions)
            {
                this.functions = functions;
            }

            public Map<String, String> getFunctions()
            {
                return functions;
            }
        }

        private static class JsonFunctionVisitor
                extends DefaultExpressionTraversalVisitor<Void, JsonFunctionContext>
        {
            @Override
            protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, JsonFunctionContext context)
            {
                process(node.getLeft(), context);
                process(node.getRight(), context);
                return null;
            }

            @Override
            protected Void visitNotExpression(NotExpression node, JsonFunctionContext context)
            {
                if (node.getValue() instanceof IsNullPredicate) {
                    IsNullPredicate predicate = (IsNullPredicate) node.getValue();
                    getJsonPath(predicate, context, false);
                }
                return null;
            }

            @Override
            protected Void visitIsNullPredicate(IsNullPredicate node, JsonFunctionContext context)
            {
                getJsonPath(node, context, true);
                return null;
            }

            private Void getJsonPath(IsNullPredicate predicate, JsonFunctionContext context, boolean isNull)
            {
                if (!(predicate.getValue() instanceof FunctionCall)) {
                    return null;
                }
                FunctionCall function = (FunctionCall) predicate.getValue();
                if (!function.getName().toString().equals("json_extract")) {
                    return null;
                }
                if (function.getArguments().size() != 2) {
                    return null;
                }
                if (!(function.getArguments().get(0) instanceof SymbolReference)) {
                    return null;
                }
                if (!(function.getArguments().get(1) instanceof Cast)) {
                    return null;
                }

                SymbolReference symbol = (SymbolReference) function.getArguments().get(0);
                Cast cast = (Cast) function.getArguments().get(1);
                if (!(cast.getExpression() instanceof StringLiteral)) {
                    return null;
                }

                StringLiteral literal = (StringLiteral) cast.getExpression();
                StringBuilder builder = new StringBuilder(isNull ? "^" : "");
                builder.append(literal.getValue());
                context.getFunctions().put(symbol.getName(), builder.toString());
                return null;
            }
        }
    }
}
