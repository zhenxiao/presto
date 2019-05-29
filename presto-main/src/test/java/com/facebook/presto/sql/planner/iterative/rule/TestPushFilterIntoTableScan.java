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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownExpressionVisitor;
import com.facebook.presto.spi.pipeline.PushDownFunction;
import com.facebook.presto.spi.pipeline.PushDownInExpression;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.PushDownLogicalBinaryExpression;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BasePushDownRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushFilterIntoTableScan
        extends BasePushDownRuleTest
{
    public TestPushFilterIntoTableScan()
    {
        super(new FilterPushDownMetadata());
    }

    @Test
    public void pushDownSimpleFilter()
    {
        testHelper("c1 = 5", true);
    }

    @Test
    public void pushDownComplexFilter()
    {
        testHelper("c1 = 5 AND c2 >= 20 AND log10(c1) = c2", true);
    }

    @Test
    public void noPushDownFilter()
    {
        testHelper("c1 in (2, 3)", false);
    }

    @Test
    public void testSQL()
    {
        // c1 can be pushed and pruned down
        assertPlan("select abs(c1) from pushdowncatalog.pushdownschema.test where c2 = 3", output(project(tableScan("test", ImmutableMap.of("c1", "c1")))));

        // c2 cannot be pruned down and thus we have an extra project
        assertPlan("select abs(c2) from pushdowncatalog.pushdownschema.test where c1 = 3", output(project(project(ImmutableMap.of("c2", PlanMatchPattern.expression("c2")), tableScan("test", ImmutableMap.of("c1", "c1", "c2", "c2"))))));
    }

    private void testHelper(String predicate, boolean expectedPushDown)
    {
        RuleAssert ruleAssert = assertThat(new PushFilterIntoTableScan(tester.getMetadata(), tester.getSqlParser()))
                .on(p ->
                        p.filter(expression(predicate),
                                createTestScan(p, "test")));

        if (expectedPushDown) {
            ruleAssert.matches(
                    strictTableScan("test",
                            ImmutableMap.of(
                                    "c1", "c1",
                                    "c2", "c2")));
        }
        else {
            ruleAssert.doesNotFire();
        }
    }

    private static class FilterPushDownMetadata
            extends BasedPushDownTestingMetadata
    {
        @Override
        public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
                TableScanPipeline currentPipeline, ProjectPipelineNode project)
        {
            for (PushDownExpression projectExpr : project.getExprs()) {
                if (projectExpr instanceof PushDownInputColumn) {
                    if (((PushDownInputColumn) projectExpr).getName().equalsIgnoreCase("c2")) {
                        return Optional.empty();
                    }
                    else {
                        continue; // fine to push c1
                    }
                }
                else if (projectExpr instanceof PushDownFunction) {
                    PushDownFunction function = (PushDownFunction) projectExpr;
                    if (!function.getName().equalsIgnoreCase("log10")) {
                        return Optional.empty();
                    }
                }
                else {
                    return Optional.empty();
                }
            }

            return merge(currentPipeline, project);
        }

        @Override
        public Optional<TableScanPipeline> pushFilterIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, FilterPipelineNode filter)
        {
            PushDownExpression expression = filter.getPredicate();

            boolean supported = new PushDownExpressionVisitor<Boolean, Void>()
            {
                @Override
                public Boolean visitExpression(PushDownExpression expression, Void context)
                {
                    return expression.accept(this, context);
                }

                @Override
                public Boolean visitInputColumn(PushDownInputColumn inputColumn, Void context)
                {
                    String columnName = inputColumn.getName();
                    return columnName.equalsIgnoreCase("c1") || columnName.equalsIgnoreCase("c2");
                }

                @Override
                public Boolean visitFunction(PushDownFunction function, Void context)
                {
                    return function.getName().equalsIgnoreCase("func");
                }

                @Override
                public Boolean visitLogicalBinary(PushDownLogicalBinaryExpression comparision, Void context)
                {
                    return true;
                }

                @Override
                public Boolean visitInExpression(PushDownInExpression in, Void context)
                {
                    // for testing purposes only
                    return false;
                }

                @Override
                public Boolean visitLiteral(PushDownLiteral literal, Void context)
                {
                    return true;
                }
            }.visitExpression(expression, null).booleanValue();

            if (supported) {
                return merge(currentPipeline, filter);
            }

            return Optional.empty();
        }
    }
}
