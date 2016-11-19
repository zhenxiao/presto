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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.NestedField;
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
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.isIdentityProjection;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import static java.util.Objects.requireNonNull;

public class PruneNestedFields
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

        if (!SystemSessionProperties.isPruneRowTypeFields(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new HashMap<String, NestedField>());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Map<String, NestedField>>
    {
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<String, NestedField>> context)
        {
            Map<String, NestedField> fields = context.get();
            processNestedFields(node.getOriginalConstraint(), fields);
            ImmutableMap.Builder<String, NestedField> builder = ImmutableMap.builder();
            Map<Symbol, ColumnHandle> assignments = node.getAssignments();
            for (Map.Entry<String, NestedField> entry : fields.entrySet()) {
                Symbol columnName = new Symbol(entry.getKey());
                if (assignments.containsKey(columnName)) {
                    NestedField field = new NestedField(columnName.getName(), entry.getValue().getFields());
                    builder.put(columnName.getName(), field);
                }
            }
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    assignments,
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint(),
                    Optional.of(builder.build()));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Map<String, NestedField>> context)
        {
            Map<String, NestedField> fields = context.get();
            for (Aggregation aggregation : node.getAggregations().values()) {
                FunctionCall functionCall = aggregation.getCall();
                for (Expression expression : functionCall.getArguments()) {
                    processNestedFields(expression, fields);
                }
            }
            PlanNode source = context.rewrite(node.getSource(), fields);
            return new AggregationNode(
                    node.getId(),
                    source,
                    node.getAggregations(),
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Map<String, NestedField>> context)
        {
            Map<String, NestedField> fields = context.get();
            processNestedFields(node.getPredicate(), fields);
            PlanNode source = context.rewrite(node.getSource(), fields);
            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Map<String, NestedField>> context)
        {
            Map<String, NestedField> fields = context.get();
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                Expression expression = node.getAssignments().get(outputSymbol);
                if (!isIdentityProjection(outputSymbol, expression)) {
                    processNestedFields(expression, fields);
                }
            }

            PlanNode source = context.rewrite(node.getSource(), fields);
            return new ProjectNode(node.getId(), source, node.getAssignments());
        }

        private static void processNestedFields(Expression expression, Map<String, NestedField> fields)
        {
            new NestedFieldVisitor().process(expression, new NestedFieldContext(fields, null));
        }

        private static class NestedFieldContext
        {
            private Map<String, NestedField> fields;
            private NestedField child;

            public NestedFieldContext(Map<String, NestedField> fields, NestedField child)
            {
                this.fields = fields;
                this.child = child;
            }

            public Map<String, NestedField> getFields()
            {
                return fields;
            }

            public NestedField getChild()
            {
                return child;
            }
        }

        private static class NestedFieldVisitor
                extends DefaultExpressionTraversalVisitor<Void, NestedFieldContext>
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, NestedFieldContext context)
            {
                Expression base = node.getBase();
                if (base instanceof DereferenceExpression || base instanceof Identifier || base instanceof SymbolReference) {
                    NestedField field = new NestedField(node.getField().getValue(), createFields(context.getChild()));
                    NestedFieldContext nestedContext = new NestedFieldContext(context.getFields(), field);
                    process(base, nestedContext);
                    return null;
                }
                process(base, context);
                return null;
            }

            @Override
            protected Void visitIdentifier(Identifier node, NestedFieldContext context)
            {
                String parentName = node.getValue();
                NestedField parent = new NestedField(parentName, createFields(context.getChild()));
                if (context.getFields().containsKey(parentName)) {
                    parent = NestedField.mergeFields(context.getFields().get(parentName), parent);
                }
                context.getFields().put(parentName, parent);
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, NestedFieldContext context)
            {
                String parentName = node.getName();
                NestedField parent = new NestedField(parentName, createFields(context.getChild()));
                if (context.getFields().containsKey(parentName)) {
                    parent = NestedField.mergeFields(context.getFields().get(parentName), parent);
                }
                context.getFields().put(parentName, parent);
                return null;
            }

            private Map<String, NestedField> createFields(NestedField child)
            {
                Map<String, NestedField> children = new HashMap<>();
                if (child != null) {
                    children.put(child.getName(), child);
                }
                return children;
            }
        }
    }
}
