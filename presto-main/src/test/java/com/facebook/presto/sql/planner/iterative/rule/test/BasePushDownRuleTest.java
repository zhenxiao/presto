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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
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
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.CreateTableScanPipeline;
import com.facebook.presto.sql.planner.iterative.rule.GenerateTableLayoutWithPipeline;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjections;
import com.facebook.presto.sql.planner.iterative.rule.PickTableLayout;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public abstract class BasePushDownRuleTest
{
    protected static final ConnectorId CONNECTOR_ID = new ConnectorId("pushdowncatalog");
    protected static final String SCHEMA_NAME = "pushdownschema";
    private final BasedPushDownTestingMetadata metadata;

    protected RuleTester tester;
    protected Session session;

    protected static class PushdownTestingLayoutHandle
            implements ConnectorTableLayoutHandle
    {
        private final TestingTableHandle table;
        private final Optional<TupleDomain<ColumnHandle>> constraint;
        private final Optional<TableScanPipeline> scanPipeline;

        public PushdownTestingLayoutHandle(
                TestingTableHandle table,
                Optional<TupleDomain<ColumnHandle>> constraint,
                Optional<TableScanPipeline> scanPipeline)
        {
            this.table = requireNonNull(table, "table is null");
            this.constraint = requireNonNull(constraint, "constraint is null");
            this.scanPipeline = requireNonNull(scanPipeline, "scanPipeline is null");
        }

        public TestingTableHandle getTable()
        {
            return table;
        }

        public Optional<TupleDomain<ColumnHandle>> getConstraint()
        {
            return constraint;
        }

        public Optional<TableScanPipeline> getScanPipeline()
        {
            return scanPipeline;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PushdownTestingLayoutHandle that = (PushdownTestingLayoutHandle) o;
            return Objects.equals(table, that.table) && Objects.equals(constraint, that.constraint) && Objects.equals(scanPipeline, that.scanPipeline);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, constraint, scanPipeline);
        }
    }

    protected static class BasedPushDownTestingMetadata
            extends TestingMetadata
    {
        @Override
        public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
        {
            ConnectorTableLayout layout = new ConnectorTableLayout(new PushdownTestingLayoutHandle((TestingTableHandle) table, Optional.of(constraint.getSummary()), Optional.empty()));
            return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
        {
            return new ConnectorTableLayout(handle);
        }

        @Override
        public Optional<TableScanPipeline> convertToTableScanPipeline(ConnectorSession session, ConnectorTableHandle tableHandle, TablePipelineNode tablePipelineNode)
        {
            TableScanPipeline scanPipeline = new TableScanPipeline();
            scanPipeline.addPipeline(tablePipelineNode, tablePipelineNode.getInputColumns());
            return Optional.of(scanPipeline);
        }

        @Override
        public Optional<ConnectorTableLayoutHandle> pushTableScanIntoConnectorLayoutHandle(ConnectorSession session, TableScanPipeline scanPipeline, ConnectorTableLayoutHandle
                connectorTableLayoutHandle)
        {
            PushdownTestingLayoutHandle currentHandle = (PushdownTestingLayoutHandle) connectorTableLayoutHandle;
            checkArgument(!currentHandle.getScanPipeline().isPresent(), "layout already has a scan pipeline");
            return Optional.of(new PushdownTestingLayoutHandle(currentHandle.getTable(), currentHandle.getConstraint(), Optional.of(scanPipeline)));
        }
    }

    public BasePushDownRuleTest(BasedPushDownTestingMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    protected static Optional<TableScanPipeline> merge(TableScanPipeline existingPipeline, PipelineNode pipelineNode)
    {
        List<ColumnHandle> newColumnHandles = new ArrayList<>();
        int index = 0;
        List<PipelineNode> newPipelineNodes = ImmutableList.<PipelineNode>builder().addAll(existingPipeline.getPipelineNodes()).add(pipelineNode).build();
        for (String outputColumn : pipelineNode.getOutputColumns()) {
            newColumnHandles.add(new TestingColumnHandle(outputColumn, index, pipelineNode.getRowType().get(index)));
            index++;
        }

        return Optional.of(new TableScanPipeline(newPipelineNodes, newColumnHandles));
    }

    protected TableScanNode createTestScan(PlanBuilder planBuilder, String tableName)
    {
        return createTestScan(planBuilder, metadata.getTableHandle(session.toConnectorSession(CONNECTOR_ID), new SchemaTableName(SCHEMA_NAME, tableName)));
    }

    private TableScanNode createTestScan(PlanBuilder planBuilder, ConnectorTableHandle tableHandle)
    {
        ConnectorSession connectorSession = session.toConnectorSession(CONNECTOR_ID);
        Map<String, ColumnHandle> columnHandlesMap = metadata.getColumnHandles(connectorSession, tableHandle);
        List<String> columns = ImmutableList.copyOf(columnHandlesMap.keySet());
        List<ColumnHandle> columnHandles = columns.stream().map(columnHandlesMap::get).collect(toImmutableList());
        List<Type> types = columnHandles.stream().map(h -> metadata.getColumnMetadata(session.toConnectorSession(CONNECTOR_ID), tableHandle, h).getType()).collect(toImmutableList());
        List<Symbol> symbols = Streams.zip(columns.stream(), types.stream(), (columnName, type) -> planBuilder.symbol(columnName, type)).collect(toImmutableList());
        Map<Symbol, ColumnHandle> symbolColumnHandleMap = IntStream.range(0, symbols.size()).boxed().collect(Collectors.toMap(symbols::get, columnHandles::get));
        TableScanPipeline scanPipeline = new TableScanPipeline();
        TablePipelineNode pipelineNode = new TablePipelineNode(tableHandle, columnHandles, columns, types);
        scanPipeline.addPipeline(pipelineNode, pipelineNode.getInputColumns());
        PushdownTestingLayoutHandle pushdownTestingLayoutHandle = new PushdownTestingLayoutHandle((TestingTableHandle) tableHandle, Optional.empty(), Optional.empty());
        return planBuilder.tableScan(
                new TableHandle(CONNECTOR_ID, tableHandle),
                symbols,
                symbolColumnHandleMap,
                Optional.of(new TableLayoutHandle(CONNECTOR_ID, TestingTransactionHandle.create(), pushdownTestingLayoutHandle)),
                Optional.of(scanPipeline));
    }

    public RuleAssert assertThat(Rule rule)
    {
        LocalQueryRunner queryRunner = tester.getQueryRunner();
        return new RuleAssert(tester.getMetadata(), queryRunner.getStatsCalculator(), queryRunner.getEstimatedExchangesCostCalculator(), session, rule,
                queryRunner.getTransactionManager(), queryRunner.getAccessControl());
    }

    protected void assertPushdownPlan(String sql, PlanMatchPattern pattern, List<PlanOptimizer> otherPlanOptimizers, List<Rule<?>> pushdownRules)
    {
        ImmutableList.Builder<PlanOptimizer> planOptimizers = ImmutableList.builder();
        planOptimizers.addAll(ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(tester.getQueryRunner().getMetadata()),
                new IterativeOptimizer(
                        new RuleStatsRecorder(),
                        tester.getQueryRunner().getStatsCalculator(),
                        tester.getQueryRunner().getCostCalculator(),
                        new PickTableLayout(tester.getQueryRunner().getMetadata(), tester.getSqlParser()).rules()),
                new IterativeOptimizer(
                        new RuleStatsRecorder(),
                        tester.getQueryRunner().getStatsCalculator(),
                        tester.getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections()))));
        planOptimizers.addAll(otherPlanOptimizers);
        ImmutableSet.Builder<Rule<?>> allPushdownRules = ImmutableSet.builder();
        allPushdownRules.add(new CreateTableScanPipeline(tester.getMetadata()));
        allPushdownRules.addAll(pushdownRules);
        planOptimizers.add(new IterativeOptimizer(
                new RuleStatsRecorder(),
                tester.getQueryRunner().getStatsCalculator(),
                tester.getQueryRunner().getCostCalculator(),
                allPushdownRules.build()));
        planOptimizers.add(new IterativeOptimizer(
                new RuleStatsRecorder(),
                tester.getQueryRunner().getStatsCalculator(),
                tester.getQueryRunner().getEstimatedExchangesCostCalculator(),
                ImmutableSet.of(
                        new InlineProjections(),
                        new RemoveRedundantIdentityProjections())));
        planOptimizers.add(new UnaliasSymbolReferences());
        planOptimizers.add(new PruneUnreferencedOutputs(tester.getQueryRunner().getMetadata()));
        planOptimizers.add(new IterativeOptimizer(
                new RuleStatsRecorder(),
                tester.getQueryRunner().getStatsCalculator(),
                tester.getQueryRunner().getCostCalculator(),
                ImmutableSet.of(new GenerateTableLayoutWithPipeline(tester.getMetadata()))));
        assertPlan(sql, pattern, planOptimizers.build());
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = tester.getQueryRunner().getPlanOptimizers(true);

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        tester.getQueryRunner().inTransaction(transactionSession -> {
            Plan actualPlan = tester.getQueryRunner().createPlan(transactionSession, sql, optimizers, stage, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, tester.getMetadata(), tester.getQueryRunner().getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();

        session = testSessionBuilder()
                .setCatalog(CONNECTOR_ID.getCatalogName())
                .setSchema(SCHEMA_NAME)
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

        ConnectorSession connectorSession = session.toConnectorSession(CONNECTOR_ID);
        getTestTables().forEach((tableName, tableColumns) -> {
            metadata.createTable(connectorSession, new ConnectorTableMetadata(new SchemaTableName(SCHEMA_NAME, tableName), tableColumns), false);
        });
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    public Map<String, List<ColumnMetadata>> getTestTables()
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.add(new ColumnMetadata("c1", INTEGER));
        columns.add(new ColumnMetadata("c2", INTEGER));
        return ImmutableMap.of("test", columns.build());
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
