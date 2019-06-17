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

package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.sanity.PlanSanityChecker;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.transaction.NoOpTransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.ENABLE_PLAN_CACHING;
import static org.testng.Assert.assertEquals;

public class TestSqlCachingPlanner
{
    private static final int REPEATED_QUERIES = 5;
    private final SqlParser sqlParser = new SqlParser();
    private final QueryPreparer queryPreparer = new QueryPreparer(sqlParser);
    private final StatsCalculator testingStatsCalculator = (node, sourceStats, lookup, session, types) -> new PlanNodeStatsEstimate(1, ImmutableMap.of());
    private final CostCalculator testingCostCalculator = (node, stats, session, types) -> new PlanNodeCostEstimate(1, 1, 1);
    private final Metadata metadata = createMetadata();

    public SqlCachingPlanner createSqlCachingPlanner(Duration cacheDuration, int planCacheSize)
    {
        return new SqlCachingPlanner(sqlParser, ImmutableList.of(), testingStatsCalculator, testingCostCalculator, cacheDuration, planCacheSize, new PlanSanityChecker(true));
    }

    private static Metadata createMetadata()
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(typeRegistry);
        FeaturesConfig featuresConfig = new FeaturesConfig();
        return new MetadataManager(
                featuresConfig,
                typeRegistry,
                blockEncodingManager,
                new SessionPropertyManager(new SystemSessionProperties(new QueryManagerConfig(), new TaskManagerConfig(), new MemoryManagerConfig(), featuresConfig)),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                new NoOpTransactionManager());
    }

    private CachingPlanner.PlanDetails plan(SqlCachingPlanner planner, String sql)
    {
        Session session = TestingSession.testSessionBuilder().setSystemProperty(ENABLE_PLAN_CACHING, "true").build();
        QueryPreparer.PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, sql);

        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, new AllowAllAccessControl(), Optional.empty(), preparedQuery.getParameters(), WarningCollector.NOOP);
        Analysis analysis = analyzer.analyze(preparedQuery.getStatement(), false);
        return planner.getPlanDetails(analysis, session, WarningCollector.NOOP, metadata, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
    }

    private void checkHitCount(String sql, boolean expectedToCache)
    {
        SqlCachingPlanner planner = createSqlCachingPlanner(new Duration(1, TimeUnit.MINUTES), 10);
        for (int i = 0; i < REPEATED_QUERIES; ++i) {
            plan(planner, sql);
        }
        assertEquals(planner.getHitCount(), expectedToCache ? REPEATED_QUERIES - 1 : 0);
    }

    @DataProvider
    public Object[][] cacheable()
    {
        return new Object[][] {{"select 1"}, {"select count(*) from (select 2 as v) subq where v != 1"}, {"select count(*) from (select random() as v) subq where v != 1"}};
    }

    @DataProvider
    public Object[][] notCacheable()
    {
        return new Object[][] {{"select current_time"}, {"select count(*) from (select now() as v) subq where v = now() - interval '1' day"}};
    }

    @Test(dataProvider = "cacheable")
    public void testQueryIsCached(String sql)
    {
        checkHitCount(sql, true);
    }

    @Test(dataProvider = "notCacheable")
    public void testQueryIsNotCached(String sql)
    {
        checkHitCount(sql, false);
    }
}
