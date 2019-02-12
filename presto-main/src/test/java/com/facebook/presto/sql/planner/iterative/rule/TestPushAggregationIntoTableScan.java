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
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Node;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BasePushDownRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.pipeline.AggregationPipelineNode.ExprType.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;

public class TestPushAggregationIntoTableScan
        extends BasePushDownRuleTest
{
    public TestPushAggregationIntoTableScan()
    {
        super(new AggregationPushDownMetadata());
    }

    @Test
    public void pushDownCompleteAggregation()
    {
        testHelper(false, true, true);
        testHelper(false, false, true);

        testHelper(false, true, false);
        testHelper(false, false, false);
    }

    @Test
    public void pushDownPartialAggregation()
    {
        testHelper(true, true, false);
        testHelper(true, false, false);
    }

    private void testHelper(boolean partial, boolean expectedPushdown, boolean hasGroupBy)
    {
        Expression pushDownExpression = expectedPushdown ? expression("count(c1)") : expression("sum(c1)");

        if (partial) {
            // SQL: select count(c1) from table(c1, c2) or select sum(c1) from table(c1, c2)
            RuleAssert ruleAssert = assertThat(new PushPartialAggregationIntoTableScan(tester.getMetadata()))
                    .on(p -> {
                        Symbol c1 = p.symbol("c1", INTEGER);
                        Symbol c2 = p.symbol("c2", INTEGER);
                        return p.aggregation(
                                b -> b.globalGrouping()
                                        .step(AggregationNode.Step.PARTIAL)
                                        .addAggregation(p.symbol("cnt"), pushDownExpression, ImmutableList.of(BIGINT))
                                        .source(
                                                p.tableScan(
                                                        new TableHandle(
                                                                CONNECTOR_ID,
                                                                new TestingTableHandle(new SchemaTableName("schema", "partial"))),
                                                        ImmutableList.of(c1, c2),
                                                        ImmutableMap.of(
                                                                c1, new TestingMetadata.TestingColumnHandle("c1", 0, INTEGER),
                                                                c2, new TestingMetadata.TestingColumnHandle("c2", 1, INTEGER)))));
                    });

            if (expectedPushdown) {
                ruleAssert.matches(
                        strictTableScan("partial",
                                ImmutableMap.of("cnt", "cnt")));
            }
            else {
                ruleAssert.doesNotFire();
            }
        }
        else {
            // SQL: select c2, count(c1) from table(c1, c2) group by c2 OR select count(c1) from table(c1, c2)
            RuleAssert ruleAssert = assertThat(new PushAggregationIntoTableScan(tester.getMetadata()))
                    .on(p -> {
                        Symbol c1 = p.symbol("c1", INTEGER);
                        Symbol c2 = p.symbol("c2", INTEGER);
                        return p.aggregation(
                                a -> (hasGroupBy ? a.singleGroupingSet(c2) : a.globalGrouping())
                                        .step(AggregationNode.Step.FINAL)
                                        .addAggregation(p.symbol("cnt"), pushDownExpression, ImmutableList.of(BIGINT))
                                        .source(
                                                p.gatheringExchange(
                                                        REMOTE,
                                                        p.gatheringExchange(
                                                                LOCAL,
                                                                p.aggregation(
                                                                        b -> (hasGroupBy ? b.singleGroupingSet(c2) : b.globalGrouping())
                                                                                .step(AggregationNode.Step.PARTIAL)
                                                                                .addAggregation(p.symbol("cnt"), pushDownExpression, ImmutableList.of(BIGINT))
                                                                                .source(
                                                                                        p.tableScan(
                                                                                                new TableHandle(
                                                                                                        CONNECTOR_ID,
                                                                                                        new TestingTableHandle(new SchemaTableName("schema",
                                                                                                                hasGroupBy ? "completewithgroupby" : "completewithnogroupby"))),
                                                                                                ImmutableList.of(c1, c2),
                                                                                                ImmutableMap.of(
                                                                                                        c1, new TestingMetadata.TestingColumnHandle("c1", 0, INTEGER),
                                                                                                        c2, new TestingMetadata.TestingColumnHandle("c2", 1, INTEGER)))))))));
                    });

            if (expectedPushdown) {
                ruleAssert.matches(
                        exchange(
                                strictTableScan(hasGroupBy ? "completewithgroupby" : "completewithnogroupby",
                                        hasGroupBy ? ImmutableMap.of("c2", "c2", "cnt", "cnt") : ImmutableMap.of("cnt", "cnt"))));
            }
            else {
                ruleAssert.doesNotFire();
            }
        }
    }

    private static class AggregationPushDownMetadata
            extends TestingMetadata
    {
        @Override
        public Optional<TableScanPipeline> pushAggregationIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, AggregationPipelineNode aggregation)
        {
            for (Node node : aggregation.getNodes()) {
                if (node.getExprType() == AGGREGATE) {
                    AggregationPipelineNode.Aggregation aggNode = (AggregationPipelineNode.Aggregation) node;
                    if (!aggNode.getFunction().equalsIgnoreCase("count")) {
                        return Optional.empty();
                    }
                }
            }

            return merge(currentPipeline, aggregation);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            TestingTableHandle testingTableHandle = (TestingTableHandle) tableHandle;

            List<ColumnMetadata> columns = new ArrayList<>();

            if (testingTableHandle.getTableName().getTableName().equals("partial")) {
                columns.add(new ColumnMetadata("cnt", BIGINT));
            }
            else if (testingTableHandle.getTableName().getTableName().equalsIgnoreCase("completewithgroupby")) {
                columns.add(new ColumnMetadata("c2", INTEGER));
                columns.add(new ColumnMetadata("cnt", BIGINT));
            }
            else if (testingTableHandle.getTableName().getTableName().equalsIgnoreCase("completewithnogroupby")) {
                columns.add(new ColumnMetadata("cnt", BIGINT));
            }

            return new ConnectorTableMetadata(testingTableHandle.getTableName(), columns);
        }
    }
}
