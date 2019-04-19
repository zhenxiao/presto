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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class BasePushDownRuleTest
{
    protected static final ConnectorId CONNECTOR_ID = new ConnectorId("pushdowncatalog");

    private final TestingMetadata metadata;

    protected RuleTester tester;
    protected Session session;

    public BasePushDownRuleTest(TestingMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    protected static Optional<TableScanPipeline> merge(TableScanPipeline existingPipeline, PipelineNode pipelineNode)
    {
        List<ColumnHandle> newColumnHandles = new ArrayList<>();
        int index = 0;
        for (String outputColumn : pipelineNode.getOutputColumns()) {
            newColumnHandles.add(new TestingColumnHandle(outputColumn, index, pipelineNode.getRowType().get(index)));
            index++;
        }

        existingPipeline.addPipeline(pipelineNode, newColumnHandles);

        return Optional.of(existingPipeline);
    }

    protected TableScanNode createTestScan(PlanBuilder planBuilder)
    {
        return createTestScan(planBuilder, new TestingTableHandle());
    }

    protected TableScanNode createTestScan(PlanBuilder planBuilder, ConnectorTableHandle tableHandle)
    {
        Symbol c1 = planBuilder.symbol("c1", INTEGER);
        Symbol c2 = planBuilder.symbol("c2", INTEGER);

        return planBuilder.tableScan(
                new TableHandle(CONNECTOR_ID, tableHandle),
                ImmutableList.of(c1, c2),
                ImmutableMap.of(
                        c1, new TestingColumnHandle("c1", 0, INTEGER),
                        c2, new TestingColumnHandle("c2", 1, INTEGER)),
                Optional.empty(),
                createTestScanPipeline());
    }

    protected Optional<TableScanPipeline> createTestScanPipeline()
    {
        TableScanPipeline scanPipeline = new TableScanPipeline();

        TablePipelineNode pipelineNode = new TablePipelineNode(
                new TestingTableHandle(),
                ImmutableList.of(
                        new TestingColumnHandle("c1", 0, INTEGER),
                        new TestingColumnHandle("c2", 1, INTEGER)),
                ImmutableList.of("c1", "c2"),
                ImmutableList.of(INTEGER, INTEGER));

        scanPipeline.addPipeline(pipelineNode, pipelineNode.getInputColumns());

        return Optional.of(scanPipeline);
    }

    public RuleAssert assertThat(Rule rule)
    {
        LocalQueryRunner queryRunner = tester.getQueryRunner();
        return new RuleAssert(tester.getMetadata(), queryRunner.getStatsCalculator(), queryRunner.getEstimatedExchangesCostCalculator(), session, rule,
                queryRunner.getTransactionManager(), queryRunner.getAccessControl());
    }

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();

        session = testSessionBuilder()
                .setCatalog(CONNECTOR_ID.getCatalogName())
                .setSchema("pushdownschema")
                .setSystemProperty("task_concurrency", "1")
                .build();

        tester.getQueryRunner().createCatalog(session.getCatalog().get(),
                new ConnectorFactory()
                {
                    @Override
                    public String getName()
                    {
                        return "pushdownTestPlugin";
                    }

                    @Override
                    public ConnectorHandleResolver getHandleResolver()
                    {
                        return new TestingHandleResolver();
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        return new PushDownTestConnector(metadata);
                    }
                },
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    private enum PushDownTestTransactionHandle
            implements ConnectorTransactionHandle
    {
        INSTANCE
    }

    private class PushDownTestConnector
            implements Connector
    {
        private final TestingMetadata metadata;

        PushDownTestConnector(TestingMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return PushDownTestTransactionHandle.INSTANCE;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return (ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy) -> {
                throw new UnsupportedOperationException();
            };
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return (ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) -> {
                throw new UnsupportedOperationException();
            };
        }
    }
}
