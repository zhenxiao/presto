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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitIntoTableScan.FinalLimitPushDown;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitIntoTableScan.PartialLimitPushDown;
import com.facebook.presto.sql.planner.iterative.rule.test.BasePushDownRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.testing.TestingMetadata;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;

public class TestPushLimitIntoTableScan
        extends BasePushDownRuleTest
{
    public TestPushLimitIntoTableScan()
    {
        super(new LimitPushDownMetadata());
    }

    @Test
    public void pushDownPartialLimitIntoTableScan()
    {
        testHelper(getSinglePhaseLimitTest(50, true), true);
        testHelper(getSinglePhaseLimitTest(20000, true), false);
    }

    @Test
    public void pushDownFinalLimitIntoTableScan()
    {
        testHelper(getSinglePhaseLimitTest(50, false), true);
        testHelper(getSinglePhaseLimitTest(20000, false), false);
    }

    private RuleAssert getSinglePhaseLimitTest(long limit, boolean partial)
    {
        // select c1, c2 from table(c1, c2) limit n (and there are multiple splits and limits (final, partial) are on each side of the exchange)
        return assertThat(partial ? new PartialLimitPushDown(tester.getMetadata()) : new FinalLimitPushDown(tester.getMetadata()))
                .on(p -> p.limit(limit, partial, createTestScan(p)));
    }

    private void testHelper(RuleAssert ruleAssert, boolean expectedPushdown)
    {
        if (expectedPushdown) {
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

    private static class LimitPushDownMetadata
            extends TestingMetadata
    {
        @Override
        public Optional<TableScanPipeline> pushLimitIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, LimitPipelineNode limit)
        {
            // for testing purposes, limit the size to 100
            if (limit.getLimit() <= 100) {
                return merge(currentPipeline, limit);
            }

            return Optional.empty();
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            List<ColumnMetadata> columns = new ArrayList<>();
            columns.add(new ColumnMetadata("c1", INTEGER));
            columns.add(new ColumnMetadata("c2", INTEGER));

            return new ConnectorTableMetadata(new SchemaTableName("schema", "test"), columns);
        }
    }
}
