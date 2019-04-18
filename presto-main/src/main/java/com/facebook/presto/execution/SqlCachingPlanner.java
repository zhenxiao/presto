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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.InputExtractor;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.OutputExtractor;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.sanity.PlanSanityChecker;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class SqlCachingPlanner
        implements CachingPlanner
{
    private final Cache<CacheKey, PlanDetails> cache;

    private final SqlParser sqlParser;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final PlanSanityChecker planSanityChecker;
    private final List<PlanOptimizer> planOptimizers;

    @Inject
    public SqlCachingPlanner(SqlParser sqlParser, PlanOptimizers planOptimizers, StatsCalculator statsCalculator, CostCalculator costCalculator, FeaturesConfig featuresConfig)
    {
        this(sqlParser, planOptimizers.get(), statsCalculator, costCalculator, featuresConfig.getPlanCacheDuration(), featuresConfig.getPlanCacheSize(), PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER);
    }

    public SqlCachingPlanner(SqlParser sqlParser, List<PlanOptimizer> planOptimizers, StatsCalculator statsCalculator, CostCalculator costCalculator, Duration cacheDuration, int planCacheSize, PlanSanityChecker planSanityChecker)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.planSanityChecker = planSanityChecker;
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite((long) cacheDuration.convertTo(TimeUnit.SECONDS).getValue(), TimeUnit.SECONDS)
                .maximumSize(planCacheSize)
                .recordStats()
                .build();
    }

    @Override
    public PlanDetails getPlanAndStuff(Analysis analysis, Session session, WarningCollector warningCollector, Metadata metadata, LogicalPlanner.Stage stage)
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Statement statement = analysis.getStatement();
        Callable<PlanDetails> planGetter = () -> {
            Plan plan = new LogicalPlanner(session, planOptimizers, planSanityChecker, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, warningCollector).plan(analysis, stage);
            List<Input> inputs = new InputExtractor(metadata, session).extractInputs(plan.getRoot());
            Optional<Output> output = new OutputExtractor().extractOutput(plan.getRoot());
            return new PlanDetails(plan, inputs, output);
        };
        PlanDetails planDetails;
        boolean useCachedPlan = SystemSessionProperties.isPlanCachingEnabled(session) && statement instanceof Query;
        if (useCachedPlan) {
            try {
                planDetails = cache.get(new CacheKey(statement, stage), planGetter);
            }
            catch (ExecutionException e) {
                Throwables.throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        else {
            try {
                planDetails = planGetter.call();
            }
            catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return planDetails;
    }

    @Managed
    public double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    private static class CacheKey
    {
        final Statement statement;
        final LogicalPlanner.Stage stage;

        CacheKey(Statement statement, LogicalPlanner.Stage stage)
        {
            this.statement = statement;
            this.stage = stage;
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
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(statement, cacheKey.statement) &&
                    stage == cacheKey.stage;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(statement, stage);
        }
    }
}
