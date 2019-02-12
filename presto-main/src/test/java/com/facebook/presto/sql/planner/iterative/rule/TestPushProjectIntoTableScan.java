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

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownFunction;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BasePushDownRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;

public class TestPushProjectIntoTableScan
        extends BasePushDownRuleTest
{
    public TestPushProjectIntoTableScan()
    {
        super(new ProjectPushDownMetadata());
    }

    @Test
    public void pushDownProjectIntoTableScan()
    {
        assertThat(new PushProjectIntoTableScan(tester.getMetadata()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1", INTEGER);
                    Symbol c2 = p.symbol("c2", INTEGER);
                    return p.project(
                            Assignments.of(
                                    c1, c1.toSymbolReference(),
                                    p.symbol("e1", INTEGER), p.expression("fun(c2)")),
                            p.tableScan(
                                    new TableHandle(
                                            CONNECTOR_ID,
                                            new TestingTableHandle()),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TestingMetadata.TestingColumnHandle("c1", 0, INTEGER),
                                            c2, new TestingMetadata.TestingColumnHandle("c2", 1, INTEGER))));
                })
                .matches(
                        strictTableScan("test",
                                ImmutableMap.of(
                                        "c1", "c1",
                                        "e1", "e1")));
    }

    @Test
    public void noPushDownProjectIntoTableScan()
    {
        assertThat(new PushProjectIntoTableScan(tester.getMetadata()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1");
                    Symbol c2 = p.symbol("c2");
                    return p.project(
                            Assignments.of(
                                    p.symbol("c1"), c1.toSymbolReference(),
                                    p.symbol("e1"), p.expression("nonFun(c2)")),
                            p.tableScan(
                                    new TableHandle(
                                            CONNECTOR_ID,
                                            new TestingTableHandle()),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TestingMetadata.TestingColumnHandle("c1", 0, INTEGER),
                                            c2, new TestingMetadata.TestingColumnHandle("c2", 1, INTEGER))));
                }).doesNotFire();
    }

    private static class ProjectPushDownMetadata
            extends TestingMetadata
    {
        @Override
        public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
                TableScanPipeline currentPipeline, ProjectPipelineNode project)
        {
            for (PushDownExpression projectExpr : project.getExprs()) {
                if (projectExpr instanceof PushDownInputColumn) {
                    continue;
                }
                else if (projectExpr instanceof PushDownFunction) {
                    PushDownFunction function = (PushDownFunction) projectExpr;
                    if (!function.getName().equalsIgnoreCase("fun")) {
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
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            List<ColumnMetadata> columns = new ArrayList<>();
            columns.add(new ColumnMetadata("c1", INTEGER));
            columns.add(new ColumnMetadata("e1", INTEGER));

            return new ConnectorTableMetadata(new SchemaTableName("schema", "test"), columns);
        }
    }
}
