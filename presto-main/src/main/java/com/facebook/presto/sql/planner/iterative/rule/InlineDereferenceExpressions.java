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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.BaseExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InlineDereferenceExpressions implements Rule
{
    private static final Pattern PATTERN = Pattern.node(ProjectNode.class);
    private static final BaseExtractor BASE_EXTRACTOR = new BaseExtractor();

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!SystemSessionProperties.isDereferenceExpressionPushDown(session)) {
            return Optional.empty();
        }
        ProjectNode parent = (ProjectNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode child = (ProjectNode) source;
        Set<Symbol> childSymbols = child.getAssignments().getSymbols();
        Function<Expression, Boolean> isBaseSymbolInChild = expression -> expression instanceof DereferenceExpression && childSymbols.contains(BASE_EXTRACTOR.process(expression));
        Set<Symbol> dereferenceSymbols = parent.getAssignments().entrySet().stream().filter(entry -> isBaseSymbolInChild.apply(entry.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
        if (dereferenceSymbols.isEmpty()) {
            return Optional.empty();
        }

        Assignments newParentAssignments = Assignments.builder().putAll(parent.getAssignments().filter(symbol -> !dereferenceSymbols.contains(symbol))).putIdentities(dereferenceSymbols).build();
        Assignments newChildAssignments = Assignments.builder().putAll(child.getAssignments()).putAll(parent.getAssignments().filter(dereferenceSymbols::contains)).build();
        return Optional.of(
                new ProjectNode(
                        parent.getId(),
                        new ProjectNode(
                                child.getId(),
                                child.getSource(),
                                newChildAssignments),
                        newParentAssignments));
    }
}
