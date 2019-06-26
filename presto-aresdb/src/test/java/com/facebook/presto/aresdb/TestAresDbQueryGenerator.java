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
package com.facebook.presto.aresdb;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.Session;
import com.facebook.presto.aresdb.query.AresDbQueryGenerator;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.JoinPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.PushDownExpressionGenerator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.rule.PushDownUtils;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.aresdb.AresDbColumnHandle.AresDbColumnType.REGULAR;
import static com.facebook.presto.aresdb.AresDbTestUtils.pipeline;
import static com.facebook.presto.aresdb.AresDbTestUtils.scan;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.PushdownTestUtils.agg;
import static com.facebook.presto.testing.PushdownTestUtils.cols;
import static com.facebook.presto.testing.PushdownTestUtils.columnHandles;
import static com.facebook.presto.testing.PushdownTestUtils.expression;
import static com.facebook.presto.testing.PushdownTestUtils.filter;
import static com.facebook.presto.testing.PushdownTestUtils.limit;
import static com.facebook.presto.testing.PushdownTestUtils.pdExpr;
import static com.facebook.presto.testing.PushdownTestUtils.project;
import static com.facebook.presto.testing.PushdownTestUtils.types;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestAresDbQueryGenerator
{
    // Test table and related info
    private static AresDbTableHandle aresdbTable = new AresDbTableHandle(new AresDbConnectorId("connId"), "tbl", Optional.of("secondsSinceEpoch"), Optional.of(BIGINT), Optional.of(new Duration(2, TimeUnit.DAYS)));
    private static AresDbTableHandle joinTable = new AresDbTableHandle(new AresDbConnectorId("connId"), "dim", Optional.empty(), Optional.empty(), Optional.empty());
    private static AresDbColumnHandle regionId = new AresDbColumnHandle("regionId", BIGINT, REGULAR);
    private static AresDbColumnHandle city = new AresDbColumnHandle("city", VARCHAR, REGULAR);
    private static AresDbColumnHandle cityId = new AresDbColumnHandle("cityId", BIGINT, REGULAR);
    private static AresDbColumnHandle cityZipCode = new AresDbColumnHandle("zip", BIGINT, REGULAR);
    private static AresDbColumnHandle cityPopulation = new AresDbColumnHandle("population", BIGINT, REGULAR);
    private static AresDbColumnHandle fare = new AresDbColumnHandle("fare", DOUBLE, REGULAR);
    private static AresDbColumnHandle secondsSinceEpoch = new AresDbColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);

    private static void testAQL(TableScanPipeline scanPipeline, String expectedAQL, Optional<ConnectorSession> session)
    {
        AresDbQueryGeneratorContext.AugmentedAQL augmentedAQL = AresDbQueryGenerator.generate(scanPipeline, Optional.empty(), Optional.empty(), session);
        assertEquals(JSONObject.parse(augmentedAQL.getAql()), JSONObject.parse(expectedAQL), augmentedAQL.getAql());
    }

    private static void testAQL(TableScanPipeline scanPipeline, String expectedAQL)
    {
        testAQL(scanPipeline, expectedAQL, Optional.empty());
    }

    private static void testUnaryAggregationHelper(AggregationPipelineNode.Aggregation aggregation, ColumnHandle aggInputColHandle, String aggInputColumnName,
            Type aggInputDataType, String expectedAggOutput)
    {
        checkArgument(!aggInputColumnName.equalsIgnoreCase("regionid") && !aggInputColumnName.equalsIgnoreCase("city"));
        AggregationPipelineNode.GroupByColumn groupByRegionId = new AggregationPipelineNode.GroupByColumn("regionid", "regionid", BIGINT);
        AggregationPipelineNode.GroupByColumn groupByCityId = new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR);
        String expectedAggOutputFormat = "{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"table\":\"tbl\"}";
        // `select agg from tbl group by regionId`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, aggInputColHandle)),
                agg(ImmutableList.of(aggregation, groupByRegionId), false)),
                format(expectedAggOutputFormat, expectedAggOutput));

        // `select regionid, agg from tbl group by regionId`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(aggInputColHandle, regionId)),
                agg(ImmutableList.of(aggregation, groupByRegionId), false)),
                format(expectedAggOutputFormat, expectedAggOutput));

        // `select regionid, agg, city from tbl group by regionId, city`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, aggInputColHandle, city)),
                agg(ImmutableList.of(groupByRegionId, aggregation, groupByCityId), false)),
                format("{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"table\":\"tbl\"}", expectedAggOutput));

        // `select regionid, agg, city from tbl group by regionId where regionid > 20`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, aggInputColHandle, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", aggInputColumnName, "city"), types(BIGINT, aggInputDataType, VARCHAR)),
                agg(ImmutableList.of(groupByRegionId, aggregation, groupByCityId), false)),
                format("{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\"}", expectedAggOutput));

        // `select regionid, agg, city from tbl group by regionId where secondssinceepoch between 200 and 300 and regionid >= 40`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, city, secondsSinceEpoch, aggInputColHandle)),
                filter(pdExpr("secondssinceepoch between 200 and 300 and regionid >= 40"), cols("regionid", "city", "secondssinceepoch", aggInputColumnName),
                        types(BIGINT, VARCHAR, BIGINT, aggInputDataType)),
                agg(ImmutableList.of(groupByRegionId, aggregation, groupByCityId), false)),
                format("{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"rowFilters\":[\"(((secondsSinceEpoch >= 200) AND (secondsSinceEpoch <= 300)) AND (regionId >= 40))\"],\"table\":\"tbl\"}", expectedAggOutput));
    }

    @Test
    public void testJoin()
    {
        PipelineNode aggNode = agg(ImmutableList.of(new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR), new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "avg", "fare_avg", DOUBLE)), false);
        List<ColumnHandle> joinScanColumns = columnHandles(cityId, cityPopulation, cityZipCode);
        TableScanPipeline joinPipeline = pipeline(
                scan(joinTable, joinScanColumns),
                createFilterForExpression(joinScanColumns, "population > 1000 and cityid not in (1, 2, 3)", cols("cityid", "population", "zip")));
        List<ColumnHandle> leftScanColumns = columnHandles(regionId, city, fare, secondsSinceEpoch);
        TableScanPipeline leftPipeline = pipeline(
                scan(aresdbTable, leftScanColumns),
                createFilterForExpression(leftScanColumns, "secondssinceepoch between 100 and 200 or secondssinceepoch between 175 and 275 and fare > 10 and city != 'DEL'", cols("regionid", "city", "fare")),
                new JoinPipelineNode(Optional.empty(), cols("city", "fare", "zip"), ImmutableList.of(VARCHAR, DOUBLE, BIGINT), joinTable, Optional.of(joinPipeline), ImmutableList.of(new JoinPipelineNode.EquiJoinClause(new PushDownInputColumn(BIGINT.getTypeSignature(), "regionid"), new PushDownInputColumn(BIGINT.getTypeSignature(), "cityid"))), JoinPipelineNode.JoinType.INNER),
                filter(pdExpr("zip != 94587"), cols("city", "fare"), ImmutableList.of(VARCHAR, DOUBLE)),
                aggNode);
        testAQL(leftPipeline, "{\"dimensions\":[{\"sqlExpression\":\"city\"}],\"joins\":[{\"alias\":\"dim\",\"conditions\":[\"regionId = dim.cityId\"],\"table\":\"dim\"}],\"measures\":[{\"sqlExpression\":\"avg(fare)\"}],\"rowFilters\":[\"(((secondsSinceEpoch >= 100) AND (secondsSinceEpoch <= 200)) OR ((((secondsSinceEpoch >= 175) AND (secondsSinceEpoch <= 275)) AND (fare > 10)) AND (city <> 'DEL')))\",\"((dim.population > 1000) AND ! (dim.cityId IN (1, 2, 3)))\",\"(dim.zip <> 94587)\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"100\",\"to\":\"275\"}}");
    }

    @Test
    public void testSimpleSelectStar()
    {
        testAQL(pipeline(scan(aresdbTable, columnHandles(regionId, city, fare, secondsSinceEpoch))),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"fare\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\"}");
    }

    @Test
    public void testSimplePartialColumnSelection()
    {
        testAQL(pipeline(scan(aresdbTable, columnHandles(regionId, secondsSinceEpoch))),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\"}");
    }

    @Test
    public void testSimpleSelectWithLimit()
    {
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, city, fare)),
                limit(50, true, cols("regionid", "city", "fare"), types(BIGINT, VARCHAR, DOUBLE))),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"fare\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\"}");
    }

    @Test
    public void testMultipleFilters()
    {
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch > 20"), cols("city", "secondssinceepoch"), types(VARCHAR, BIGINT)),
                filter(pdExpr("city < 200 AND city > 10"), cols("city", "secondssinceepoch"), types(VARCHAR, BIGINT))),
                "{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(secondsSinceEpoch > 20)\",\"((city < 200) AND (city > 10))\"],\"table\":\"tbl\"}");
    }

    @Test
    public void testSimpleSelectWithFilter()
    {
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch > 20"), cols("city", "secondssinceepoch"), types(VARCHAR, BIGINT))),
                "{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(secondsSinceEpoch > 20)\"],\"table\":\"tbl\"}");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch > 20"), cols("city", "secondssinceepoch"), types(VARCHAR, BIGINT)),
                limit(50, true, cols("city"), types(VARCHAR))),
                "{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(secondsSinceEpoch > 20)\"],\"table\":\"tbl\"}");
    }

    private static FilterPipelineNode createFilterForExpression(List<ColumnHandle> incomingColumns, String expressionSql, List<String> cols)
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        Session session = testSessionBuilder().build();
        Expression predicate = expression(expressionSql);
        PushDownUtils.ExpressionToTypeConverter typeConverter = expression -> BIGINT;
        PushDownExpression pushdownPredicate = new PushDownExpressionGenerator(typeConverter).process(predicate);
        Map<Symbol, Type> incomingTypes = incomingColumns.stream().map(c -> (AresDbColumnHandle) c).collect(toImmutableMap(ch -> new Symbol(ch.getColumnName().toLowerCase(ENGLISH)), AresDbColumnHandle::getDataType));
        DomainTranslator.ExtractionResult extractionResult = DomainTranslator.fromPredicate(metadata, session, predicate, new SymbolAllocator(incomingTypes).getTypes());
        Optional<PushDownExpression> remainingPredicate = Optional.ofNullable(new PushDownExpressionGenerator(typeConverter).process(extractionResult.getRemainingExpression()));
        Optional<TupleDomain<String>> symbolNameToDomains;

        // only do this if the remainingPushdownPredicate is not null
        if (remainingPredicate.isPresent() && !extractionResult.getTupleDomain().isAll()) {
            symbolNameToDomains = Optional.of(extractionResult.getTupleDomain().transform(Symbol::getName));
        }
        else {
            symbolNameToDomains = Optional.empty();
        }
        return new FilterPipelineNode(
                pushdownPredicate,
                cols,
                types(BIGINT, VARCHAR, BIGINT), symbolNameToDomains, remainingPredicate);
    }

    @Test
    public void testTimeFilterDefaultRetentionBounds()
    {
        long currentTime = System.currentTimeMillis() + 100; // 100 to make sure it is not aligned to second boundary;
        long retentionTime = currentTime - aresdbTable.getRetention().get().toMillis();
        long highSecondsExpected = (currentTime + 999) / 1000;
        long lowSecondsExpected = retentionTime / 1000;
        ConnectorSession session = new TestingConnectorSession("user", Optional.of("test"), Optional.empty(), UTC_KEY, ENGLISH, currentTime, ImmutableList.of(), ImmutableMap.of(), new FeaturesConfig().isLegacyTimestamp());
        List<ColumnHandle> columnHandles = columnHandles(city, regionId, secondsSinceEpoch);
        testAQL(pipeline(
                scan(aresdbTable, columnHandles),
                createFilterForExpression(columnHandles, "regionid in (3, 4)", cols("regionid", "city", "secondssinceepoch")),
                limit(50, true, cols("city"), types(VARCHAR))),
                String.format("{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(regionId IN (3, 4))\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"%d\",\"to\":\"%d\"}}", lowSecondsExpected, highSecondsExpected), Optional.of(session));
    }

    @Test
    public void testTimeFilter()
    {
        List<ColumnHandle> columnHandles = columnHandles(city, regionId, secondsSinceEpoch);
        testAQL(pipeline(
                scan(aresdbTable, columnHandles),
                createFilterForExpression(columnHandles, "regionid in (3, 4) and (secondssinceepoch between 100 and 200) and (secondssinceepoch >= 150 or secondssinceepoch < 300)", cols("regionid", "city", "secondssinceepoch")),
                limit(50, true, cols("city"), types(VARCHAR))),
                "{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(((regionId IN (3, 4)) AND ((secondsSinceEpoch >= 100) AND (secondsSinceEpoch <= 200))) AND ((secondsSinceEpoch >= 150) OR (secondsSinceEpoch < 300)))\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"100\",\"to\":\"200\"}}");
    }

    @Test
    public void testCountStarAggregation()
    {
        AggregationPipelineNode.Aggregation countStar = new AggregationPipelineNode.Aggregation(ImmutableList.of(), "count", "total", BIGINT);
        AggregationPipelineNode.GroupByColumn groupByRegionId = new AggregationPipelineNode.GroupByColumn("regionid", "regionid", BIGINT);
        AggregationPipelineNode.GroupByColumn groupByCityId = new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR);

        // `select count(*) from tbl group by regionId`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId)),
                agg(ImmutableList.of(countStar, groupByRegionId), false)),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\"}");

        // `select regionid, count(*) from tbl group by regionId`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId)),
                agg(ImmutableList.of(groupByRegionId, countStar), false)),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\"}");

        // `select regionid, count(*), city from tbl group by regionId`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, city)),
                agg(ImmutableList.of(groupByRegionId, countStar, groupByCityId), false)),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\"}");

        // `select regionid, count(*), city from tbl group by regionId where regionid > 20`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", "city"), types(BIGINT, VARCHAR)),
                agg(ImmutableList.of(groupByRegionId, countStar, groupByCityId), false)),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\"}");

        // `select regionid, count(*), city from tbl group by regionId where secondssinceepoch between 200 and 300 and regionid >= 40`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch between 200 and 300 and regionid >= 40"), cols("regionid", "city", "secondssinceepoch"), types(BIGINT, VARCHAR, BIGINT)),
                agg(ImmutableList.of(groupByRegionId, countStar, groupByCityId), false)),
                "{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"rowFilters\":[\"(((secondsSinceEpoch >= 200) AND (secondsSinceEpoch <= 300)) AND (regionId >= 40))\"],\"table\":\"tbl\"}");
    }

    @Test
    public void testUnaryAggregation()
    {
        AggregationPipelineNode.Aggregation count = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "count", "fare_total", DOUBLE);
        testUnaryAggregationHelper(count, fare, "fare", DOUBLE, "count(fare)");

        AggregationPipelineNode.Aggregation sum = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "sum", "fare_total", DOUBLE);
        testUnaryAggregationHelper(sum, fare, "fare", DOUBLE, "sum(fare)");

        AggregationPipelineNode.Aggregation min = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        testUnaryAggregationHelper(min, fare, "fare", DOUBLE, "min(fare)");

        AggregationPipelineNode.Aggregation max = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "max", "fare_max", DOUBLE);
        testUnaryAggregationHelper(max, fare, "fare", DOUBLE, "max(fare)");

        AggregationPipelineNode.Aggregation avg = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "avg", "fare_avg", DOUBLE);
        testUnaryAggregationHelper(avg, fare, "fare", DOUBLE, "avg(fare)");

        AggregationPipelineNode.Aggregation approxDistinct = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "approx_distinct", "fare_approx_distinct", DOUBLE);
        testUnaryAggregationHelper(approxDistinct, fare, "fare", DOUBLE, "countdistincthll(fare)");
    }

    @Test
    public void testApproxDistinct()
    {
        AggregationPipelineNode.Aggregation distinct = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "approx_distinct", "fare_distinct", DOUBLE);
        AggregationPipelineNode.GroupByColumn groupByRegionId = new AggregationPipelineNode.GroupByColumn("regionid", "regionid", BIGINT);
        AggregationPipelineNode.GroupByColumn groupByCityId = new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR);

        // `select agg from tbl group by regionId`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, fare)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare")), cols("regionid", "fare"), types(BIGINT, DOUBLE)),
                agg(ImmutableList.of(distinct, groupByRegionId), false)),
                format("{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"table\":\"tbl\"}"));

        // `select regionid, agg, city from tbl group by regionId where regionid > 20`
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(regionId, fare, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", "fare", "city"), types(BIGINT, DOUBLE, VARCHAR)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("city")),
                        cols("regionid", "fare", "city"), types(BIGINT, DOUBLE, VARCHAR)),
                agg(ImmutableList.of(groupByRegionId, distinct, groupByCityId), false)),
                format("{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\"}"));
    }

    @Test
    public void testAggWithUDFInGroupBy()
    {
        AggregationPipelineNode.Aggregation approxDistinct = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "approx_distinct", "approx_distinct_col", DOUBLE);
        AggregationPipelineNode.GroupByColumn groupByDay = new AggregationPipelineNode.GroupByColumn("day", "day", BIGINT);
        AggregationPipelineNode.GroupByColumn groupByCityId = new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR);
        PushDownExpression dateTrunc = pdExpr("date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        testAQL(pipeline(
                scan(aresdbTable, columnHandles(city, fare, secondsSinceEpoch)),
                project(ImmutableList.of(pdExpr("city"), pdExpr("fare"), dateTrunc),
                        cols("city", "fare", "day"), types(VARCHAR, DOUBLE, BIGINT)),
                agg(ImmutableList.of(approxDistinct, groupByDay, groupByCityId), false)),
                "{\"dimensions\":[{\"sqlExpression\":\"secondsSinceEpoch - 50\",\"timeBucketizer\":\"day\",\"timeUnit\":\"second\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"table\":\"tbl\"}");
    }

    @Test(expectedExceptions = AresDbException.class)
    public void testMultipleAggregates()
    {
        AggregationPipelineNode.Aggregation min = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        AggregationPipelineNode.Aggregation max = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "max", "fare_max", DOUBLE);
        AggregationPipelineNode.Aggregation count = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "count", "fare_total", BIGINT);
        AggregationPipelineNode.Aggregation approxDistinct = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "approx_distinct", "fare_approx_distinct", DOUBLE);
        AggregationPipelineNode.GroupByColumn groupByDay = new AggregationPipelineNode.GroupByColumn("day", "day", BIGINT);
        AggregationPipelineNode.GroupByColumn groupByCityId = new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR);
        PushDownExpression dateTrunc = pdExpr("date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");

        testAQL(pipeline(
                scan(aresdbTable, columnHandles(city, fare, secondsSinceEpoch)),
                project(ImmutableList.of(pdExpr("city"), pdExpr("fare"), dateTrunc),
                        cols("city", "fare", "day"), types(VARCHAR, DOUBLE, BIGINT)),
                agg(ImmutableList.of(min, max, count, approxDistinct, groupByDay, groupByCityId), false)),
                null);
    }
}
