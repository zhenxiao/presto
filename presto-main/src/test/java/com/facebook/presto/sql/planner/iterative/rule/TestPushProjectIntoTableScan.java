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
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownArithmeticExpression;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownFunction;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BasePushDownRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestPushProjectIntoTableScan
        extends BasePushDownRuleTest
{
    public TestPushProjectIntoTableScan()
    {
        super(new ProjectPushDownMetadata());
    }

    @Test
    public void testWithSql()
    {
        assertPushdownPlan("select abs(c1 + c2) from pushdowncatalog.pushdownschema.test",
                output(tableScan("test")),
                ImmutableList.of(),
                ImmutableList.of(new PushProjectIntoTableScan(tester.getMetadata(), tester.getSqlParser())));
    }

    @Test
    public void pushDownProjectIntoTableScan()
    {
        assertThat(new PushProjectIntoTableScan(tester.getMetadata(), tester.getSqlParser()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1", INTEGER);
                    return p.project(
                            Assignments.of(
                                    c1, c1.toSymbolReference(),
                                    p.symbol("e1", INTEGER), p.expression("cast(abs(c1 + c2) * 2 as bigint)")),
                            createTestScan(p, "test"));
                })
                .matches(tableScan("test"));
    }

    @Test
    public void noPushNonImplicitCastIntoTableScan()
    {
        assertThat(new PushProjectIntoTableScan(tester.getMetadata(), tester.getSqlParser()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1", INTEGER);
                    return p.project(
                            Assignments.of(
                                    c1, c1.toSymbolReference(),
                                    p.symbol("e1", INTEGER), p.expression("cast(abs(c1 - 1) as tinyint)")),
                            createTestScan(p, "test"));
                }).doesNotFire();
    }

    @Test
    public void noPushDownProjectIntoTableScan()
    {
        assertThat(new PushProjectIntoTableScan(tester.getMetadata(), tester.getSqlParser()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1", INTEGER);
                    return p.project(
                            Assignments.of(
                                    c1, c1.toSymbolReference(),
                                    p.symbol("e1", INTEGER), p.expression("log10(c2)")),
                            createTestScan(p, "test"));
                }).doesNotFire();
    }

    private static class ProjectPushDownMetadata
            extends BasedPushDownTestingMetadata
    {
        @Override
        public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
                TableScanPipeline currentPipeline, ProjectPipelineNode project)
        {
            for (PushDownExpression projectExpr : project.getExprs()) {
                if (projectExpr instanceof PushDownInputColumn || projectExpr instanceof PushDownArithmeticExpression) {
                    continue;
                }
                else if (projectExpr instanceof PushDownFunction) {
                    PushDownFunction function = (PushDownFunction) projectExpr;
                    if (!function.getName().equalsIgnoreCase("abs")) {
                        return Optional.empty();
                    }
                }
                else {
                    return Optional.empty();
                }
            }

            return merge(currentPipeline, project);
        }
    }
}
