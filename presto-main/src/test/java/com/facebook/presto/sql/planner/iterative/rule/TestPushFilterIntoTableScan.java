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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
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

    private void testHelper(String predicate, boolean expectedPushDown)
    {
        RuleAssert ruleAssert = assertThat(new PushFilterIntoTableScan(tester.getMetadata(), tester.getSqlParser()))
                .on(p ->
                        p.filter(expression(predicate),
                                createTestScan(p)));

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
                    if (!function.getName().equalsIgnoreCase("log10")) {
                        return Optional.empty();
                    }
                }
                else {
                    return Optional.empty();
                }
            }

            List<ColumnHandle> newColumnHandles = new ArrayList<>();
            int index = 0;
            for (String outputColumn : project.getOutputColumns()) {
                newColumnHandles.add(new TestingColumnHandle(outputColumn, index, project.getRowType().get(index)));
                index++;
            }

            TableScanPipeline pipeline = new TableScanPipeline();
            pipeline.addPipeline(project, newColumnHandles);

            return Optional.of(pipeline);
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
