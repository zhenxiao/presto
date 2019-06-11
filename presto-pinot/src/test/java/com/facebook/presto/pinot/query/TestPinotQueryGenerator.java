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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Aggregation;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.GroupByColumn;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.PinotTestUtils.agg;
import static com.facebook.presto.pinot.PinotTestUtils.cols;
import static com.facebook.presto.pinot.PinotTestUtils.columnHandles;
import static com.facebook.presto.pinot.PinotTestUtils.filter;
import static com.facebook.presto.pinot.PinotTestUtils.limit;
import static com.facebook.presto.pinot.PinotTestUtils.pdExpr;
import static com.facebook.presto.pinot.PinotTestUtils.pipeline;
import static com.facebook.presto.pinot.PinotTestUtils.project;
import static com.facebook.presto.pinot.PinotTestUtils.scan;
import static com.facebook.presto.pinot.PinotTestUtils.types;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPinotQueryGenerator
{
    // Test table and related info
    private static PinotTableHandle pinotTable = new PinotTableHandle("connId", "schema", "tbl");
    private static PinotColumnHandle regionId = new PinotColumnHandle("regionId", BIGINT, REGULAR);
    private static PinotColumnHandle city = new PinotColumnHandle("city", VARCHAR, REGULAR);
    private static PinotColumnHandle fare = new PinotColumnHandle("fare", DOUBLE, REGULAR);
    private static PinotColumnHandle secondsSinceEpoch = new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);

    private static Void testPQL(TableScanPipeline scanPipeline, String expectedPQL)
    {
        String actualPQL = PinotQueryGenerator.generatePQL(scanPipeline, Optional.empty());
        assertEquals(actualPQL, expectedPQL);
        return null;
    }

    private static void testUnaryAggregationHelper(Aggregation aggregation, ColumnHandle aggInputColHandle, String aggInputColumnName,
            Type aggInputDataType, String expectedAggOutput)
    {
        checkArgument(!aggInputColumnName.equalsIgnoreCase("regionid") && !aggInputColumnName.equalsIgnoreCase("city"));
        GroupByColumn groupByRegionId = new GroupByColumn("regionid", "regionid", BIGINT);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);

        // `select agg from tbl`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(aggInputColHandle)),
                agg(ImmutableList.of(aggregation), false)),
                format("SELECT %s FROM tbl", expectedAggOutput));

        // `select agg from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, aggInputColHandle)),
                agg(ImmutableList.of(aggregation, groupByRegionId), false)),
                format("SELECT %s FROM tbl GROUP BY regionId TOP 10000", expectedAggOutput));

        // `select regionid, agg from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(aggInputColHandle, regionId)),
                agg(ImmutableList.of(aggregation, groupByRegionId), false)),
                format("SELECT %s FROM tbl GROUP BY regionId TOP 10000", expectedAggOutput));

        // `select regionid, agg, city from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, aggInputColHandle, city)),
                agg(ImmutableList.of(groupByRegionId, aggregation, groupByCityId), false)),
                format("SELECT %s FROM tbl GROUP BY regionId, city TOP 10000", expectedAggOutput));

        // `select regionid, agg, city from tbl group by regionId where regionid > 20`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, aggInputColHandle, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", aggInputColumnName, "city"), types(BIGINT, aggInputDataType, VARCHAR)),
                agg(ImmutableList.of(groupByRegionId, aggregation, groupByCityId), false)),
                format("SELECT %s FROM tbl WHERE (regionId > 20) GROUP BY regionId, city TOP 10000", expectedAggOutput));

        // `select regionid, agg, city from tbl group by regionId where secondssinceepoch between 200 and 300 and regionid >= 40`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, city, secondsSinceEpoch, aggInputColHandle)),
                filter(pdExpr("secondssinceepoch between 200 and 300 and regionid >= 40"), cols("regionid", "city", "secondssinceepoch", aggInputColumnName),
                        types(BIGINT, VARCHAR, BIGINT, aggInputDataType)),
                agg(ImmutableList.of(groupByRegionId, aggregation, groupByCityId), false)),
                format("SELECT %s FROM tbl WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city TOP 10000", expectedAggOutput));
    }

    @Test
    public void testSimpleSelectStar()
    {
        testPQL(pipeline(scan(pinotTable, columnHandles(regionId, city, fare, secondsSinceEpoch))),
                "SELECT regionId, city, fare, secondsSinceEpoch FROM tbl LIMIT 2147483647");
    }

    @Test
    public void testSimplePartialColumnSelection()
    {
        testPQL(pipeline(scan(pinotTable, columnHandles(regionId, secondsSinceEpoch))),
                "SELECT regionId, secondsSinceEpoch FROM tbl LIMIT 2147483647");
    }

    @Test
    public void testSimpleSelectWithLimit()
    {
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, city, fare)),
                limit(50, true, cols("regionid", "city", "fare"), types(BIGINT, VARCHAR, DOUBLE))),
                "SELECT regionId, city, fare FROM tbl LIMIT 50");
    }

    @Test
    public void testSimpleSelectWithFilter()
    {
        testPQL(pipeline(
                scan(pinotTable, columnHandles(city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch > 20"), cols("city", "secondssinceepoch"), types(VARCHAR, BIGINT))),
                "SELECT city, secondsSinceEpoch FROM tbl WHERE (secondsSinceEpoch > 20) LIMIT 2147483647");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testPQL(pipeline(
                scan(pinotTable, columnHandles(city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch > 20"), cols("city", "secondssinceepoch"), types(VARCHAR, BIGINT)),
                limit(50, true, cols("city"), types(VARCHAR))),
                "SELECT city, secondsSinceEpoch FROM tbl WHERE (secondsSinceEpoch > 20) LIMIT 50");
    }

    @Test
    public void testSimpleSelectWithFilterLimitAndTupleDomainFilter()
    {
        TableScanPipeline scanPipeline = pipeline(
                scan(pinotTable, columnHandles(city, regionId)),
                filter(pdExpr("regionid > 20"), cols("city", "regionid"), types(VARCHAR, BIGINT)),
                limit(50, true, cols("city"), types(VARCHAR)));

        String actualPQL = PinotQueryGenerator.generateForSegmentSplits(scanPipeline, Optional.of("_REALTIME"), Optional.of("secondsSinceEpoch > 200"), Optional.empty()).getPql();
        assertEquals(actualPQL, "SELECT city, regionId FROM tbl_REALTIME WHERE secondsSinceEpoch > 200 AND (regionId > 20) LIMIT 50");
    }

    @Test
    public void testCountStarAggregation()
    {
        Aggregation countStar = new Aggregation(ImmutableList.of(), "count", "total", BIGINT);
        GroupByColumn groupByRegionId = new GroupByColumn("regionid", "regionid", BIGINT);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);

        // simple `select count(*) from tbl`
        testPQL(pipeline(
                scan(pinotTable, columnHandles()),
                agg(ImmutableList.of(countStar), false)),
                "SELECT count(*) FROM tbl");

        // `select count(*) from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId)),
                agg(ImmutableList.of(countStar, groupByRegionId), false)),
                "SELECT count(*) FROM tbl GROUP BY regionId TOP 10000");

        // `select regionid, count(*) from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId)),
                agg(ImmutableList.of(groupByRegionId, countStar), false)),
                "SELECT count(*) FROM tbl GROUP BY regionId TOP 10000");

        // `select regionid, count(*), city from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, city)),
                agg(ImmutableList.of(groupByRegionId, countStar, groupByCityId), false)),
                "SELECT count(*) FROM tbl GROUP BY regionId, city TOP 10000");

        // `select regionid, count(*), city from tbl group by regionId where regionid > 20`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", "city"), types(BIGINT, VARCHAR)),
                agg(ImmutableList.of(groupByRegionId, countStar, groupByCityId), false)),
                "SELECT count(*) FROM tbl WHERE (regionId > 20) GROUP BY regionId, city TOP 10000");

        // `select regionid, count(*), city from tbl group by regionId where secondssinceepoch between 200 and 300 and regionid >= 40`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, city, secondsSinceEpoch)),
                filter(pdExpr("secondssinceepoch between 200 and 300 and regionid >= 40"), cols("regionid", "city", "secondssinceepoch"), types(BIGINT, VARCHAR, BIGINT)),
                agg(ImmutableList.of(groupByRegionId, countStar, groupByCityId), false)),
                "SELECT count(*) FROM tbl WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city TOP 10000");
    }

    @Test
    public void testUnaryAggregation()
    {
        Aggregation count = new Aggregation(ImmutableList.of("fare"), "count", "fare_total", DOUBLE);
        testUnaryAggregationHelper(count, fare, "fare", DOUBLE, "count(fare)");

        Aggregation sum = new Aggregation(ImmutableList.of("fare"), "sum", "fare_total", DOUBLE);
        testUnaryAggregationHelper(sum, fare, "fare", DOUBLE, "sum(fare)");

        Aggregation min = new Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        testUnaryAggregationHelper(min, fare, "fare", DOUBLE, "min(fare)");

        Aggregation max = new Aggregation(ImmutableList.of("fare"), "max", "fare_max", DOUBLE);
        testUnaryAggregationHelper(max, fare, "fare", DOUBLE, "max(fare)");

        Aggregation avg = new Aggregation(ImmutableList.of("fare"), "avg", "fare_avg", DOUBLE);
        testUnaryAggregationHelper(avg, fare, "fare", DOUBLE, "avg(fare)");

        Aggregation approxDistinct = new Aggregation(ImmutableList.of("fare"), "approx_distinct", "fare_approx_distinct", DOUBLE);
        testUnaryAggregationHelper(approxDistinct, fare, "fare", DOUBLE, "DISTINCTCOUNTHLL(fare)");
    }

    @Test
    public void testPercentileAggregation()
    {
        Aggregation percentile = new Aggregation(ImmutableList.of("fare", "percentile"), "approx_percentile", "fare_percentile", DOUBLE);
        GroupByColumn groupByRegionId = new GroupByColumn("regionid", "regionid", BIGINT);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);

        // `select agg from tbl`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(fare)),
                project(ImmutableList.of(pdExpr("fare"), pdExpr("99")), cols("fare", "percentile"), types(DOUBLE, DOUBLE)),
                agg(ImmutableList.of(percentile), false)),
                format("SELECT %s FROM tbl", "PERCENTILEEST99(fare)"));

        // `select agg from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, fare)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("1")), cols("regionid", "fare", "percentile"), types(BIGINT, DOUBLE, DOUBLE)),
                agg(ImmutableList.of(percentile, groupByRegionId), false)),
                format("SELECT %s FROM tbl GROUP BY regionId TOP 10000", "PERCENTILEEST1(fare)"));

        // `select regionid, agg from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, fare)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("2")), cols("regionid", "fare", "percentile"), types(BIGINT, DOUBLE, DOUBLE)),
                agg(ImmutableList.of(groupByRegionId, percentile), false)),
                format("SELECT %s FROM tbl GROUP BY regionId TOP 10000", "PERCENTILEEST2(fare)"));

        // `select regionid, agg, city from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, fare, city)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("city"), pdExpr("3")),
                        cols("regionid", "fare", "city", "percentile"), types(BIGINT, DOUBLE, VARCHAR, DOUBLE)),
                agg(ImmutableList.of(groupByRegionId, percentile, groupByCityId), false)),
                format("SELECT %s FROM tbl GROUP BY regionId, city TOP 10000", "PERCENTILEEST3(fare)"));

        // `select regionid, agg, city from tbl group by regionId where regionid > 20`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, fare, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", "fare", "city"), types(BIGINT, DOUBLE, VARCHAR)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("city"), pdExpr("4")),
                        cols("regionid", "fare", "city", "percentile"), types(BIGINT, DOUBLE, VARCHAR, DOUBLE)),
                agg(ImmutableList.of(groupByRegionId, percentile, groupByCityId), false)),
                format("SELECT %s FROM tbl WHERE (regionId > 20) GROUP BY regionId, city TOP 10000", "PERCENTILEEST4(fare)"));

        // `select regionid, agg, city from tbl group by regionId where secondssinceepoch between 200 and 300 and regionid >= 40`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, city, secondsSinceEpoch, fare)),
                filter(pdExpr("secondssinceepoch between 200 and 300 and regionid >= 40"), cols("regionid", "city", "secondssinceepoch", "fare"),
                        types(BIGINT, VARCHAR, BIGINT, DOUBLE)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("city"), pdExpr("fare"), pdExpr("5")),
                        cols("regionid", "city", "fare", "percentile"), types(BIGINT, VARCHAR, DOUBLE, DOUBLE)),
                agg(ImmutableList.of(groupByRegionId, percentile, groupByCityId), false)),
                format("SELECT %s FROM tbl WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city TOP 10000", "PERCENTILEEST5(fare)"));
    }

    @Test
    public void testApproxDistinct()
    {
        Aggregation distinct = new Aggregation(ImmutableList.of("fare"), "approx_distinct", "fare_distinct", DOUBLE);
        GroupByColumn groupByRegionId = new GroupByColumn("regionid", "regionid", BIGINT);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);

        // `select agg from tbl`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(fare)),
                project(ImmutableList.of(pdExpr("fare")), cols("fare"), types(DOUBLE)),
                agg(ImmutableList.of(distinct), false)),
                format("SELECT %s FROM tbl", "DISTINCTCOUNTHLL(fare)"));

        // `select agg from tbl group by regionId`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, fare)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare")), cols("regionid", "fare"), types(BIGINT, DOUBLE)),
                agg(ImmutableList.of(distinct, groupByRegionId), false)),
                format("SELECT %s FROM tbl GROUP BY regionId TOP 10000", "DISTINCTCOUNTHLL(fare)"));

        // `select regionid, agg, city from tbl group by regionId where regionid > 20`
        testPQL(pipeline(
                scan(pinotTable, columnHandles(regionId, fare, city)),
                filter(pdExpr("regionid > 20"), cols("regionid", "fare", "city"), types(BIGINT, DOUBLE, VARCHAR)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("city")),
                        cols("regionid", "fare", "city"), types(BIGINT, DOUBLE, VARCHAR)),
                agg(ImmutableList.of(groupByRegionId, distinct, groupByCityId), false)),
                format("SELECT %s FROM tbl WHERE (regionId > 20) GROUP BY regionId, city TOP 10000", "DISTINCTCOUNTHLL(fare)"));
    }

    @Test
    public void testAggWithUDFInGroupBy()
    {
        Aggregation percentile = new Aggregation(ImmutableList.of("fare", "percentile"), "approx_percentile", "fare_percentile", DOUBLE);
        GroupByColumn groupByDay = new GroupByColumn("day", "day", BIGINT);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);
        PushDownExpression dateTrunc = pdExpr("date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        testPQL(pipeline(
                scan(pinotTable, columnHandles(city, fare, secondsSinceEpoch)),
                project(ImmutableList.of(pdExpr("city"), pdExpr("fare"), dateTrunc, pdExpr("99")),
                        cols("city", "fare", "day", "percentile"), types(VARCHAR, DOUBLE, BIGINT, DOUBLE)),
                agg(ImmutableList.of(percentile, groupByDay, groupByCityId), false)),
                "SELECT PERCENTILEEST99(fare) FROM tbl GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS'), city TOP 10000");
    }

    @Test
    public void testMultipleAggregatesWithOutGroupBy()
    {
        Aggregation min = new Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        Aggregation max = new Aggregation(ImmutableList.of("fare"), "max", "fare_max", DOUBLE);
        Aggregation count = new Aggregation(ImmutableList.of("fare"), "count", "fare_total", BIGINT);
        Aggregation percentile = new Aggregation(ImmutableList.of("fare", "percentile"), "approx_percentile", "fare_percentile", DOUBLE);

        testPQL(pipeline(
                scan(pinotTable, columnHandles(fare)),
                project(ImmutableList.of(pdExpr("fare"), pdExpr("99")),
                        cols("fare", "percentile"), types(DOUBLE, DOUBLE)),
                agg(ImmutableList.of(min, max, count, percentile), false)),
                "SELECT min(fare), max(fare), count(fare), PERCENTILEEST99(fare) FROM tbl");

        testPQL(pipeline(
                scan(pinotTable, columnHandles(fare)),
                project(ImmutableList.of(pdExpr("fare"), pdExpr("99")),
                        cols("fare", "percentile"), types(DOUBLE, DOUBLE)),
                agg(ImmutableList.of(min, max, count, percentile), false),
                limit(50, true, cols("fare_min", "fare_max", "fare_total", "fare_percentile"), types(BIGINT, BIGINT, BIGINT, DOUBLE))),
                "SELECT min(fare), max(fare), count(fare), PERCENTILEEST99(fare) FROM tbl");
    }

    @Test
    public void testMultipleAggregatesWhenAllowed()
    {
        helperTestMultipleAggregatesWithGroupBy(Optional.of(new PinotConfig().setAllowMultipleAggregations(true)));
    }

    @Test(expectedExceptions = PinotException.class)
    public void testMultipleAggregatesNotAllowed()
    {
        helperTestMultipleAggregatesWithGroupBy(Optional.of(new PinotConfig()));
    }

    private void helperTestMultipleAggregatesWithGroupBy(Optional<PinotConfig> pinotConfig)
    {
        Aggregation min = new Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        Aggregation max = new Aggregation(ImmutableList.of("fare"), "max", "fare_max", DOUBLE);
        Aggregation count = new Aggregation(ImmutableList.of("fare"), "count", "fare_total", BIGINT);
        Aggregation percentile = new Aggregation(ImmutableList.of("fare", "percentile"), "approx_percentile", "fare_percentile", DOUBLE);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);

        String actualPQL = PinotQueryGenerator.generatePQL(pipeline(
                scan(pinotTable, columnHandles(city, fare)),
                project(ImmutableList.of(pdExpr("city"), pdExpr("fare"), pdExpr("99")),
                        cols("city", "fare", "percentile"), types(VARCHAR, DOUBLE, DOUBLE)),
                agg(ImmutableList.of(min, max, count, percentile, groupByCityId), false)), pinotConfig);
        assertEquals(actualPQL, "SELECT min(fare), max(fare), count(fare), PERCENTILEEST99(fare) FROM tbl GROUP BY city TOP 10000");
    }

    @Test(expectedExceptions = PinotException.class)
    public void testMultipleAggregateGroupByWithLimitFails()
    {
        Aggregation min = new Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        Aggregation max = new Aggregation(ImmutableList.of("fare"), "max", "fare_max", DOUBLE);
        Aggregation count = new Aggregation(ImmutableList.of("fare"), "count", "fare_total", BIGINT);
        Aggregation percentile = new Aggregation(ImmutableList.of("fare", "percentile"), "approx_percentile", "fare_percentile", DOUBLE);
        GroupByColumn groupByCityId = new GroupByColumn("city", "city", VARCHAR);

        String actualPQL = PinotQueryGenerator.generatePQL(pipeline(
                scan(pinotTable, columnHandles(city, fare)),
                project(ImmutableList.of(pdExpr("city"), pdExpr("fare"), pdExpr("99")),
                        cols("city", "fare", "percentile"), types(VARCHAR, DOUBLE, DOUBLE)),
                agg(ImmutableList.of(min, max, count, percentile, groupByCityId), false),
                limit(10, false, ImmutableList.of("fare_min", "fare_max", "fare_total", "fare_percentile"), types(DOUBLE, DOUBLE, BIGINT, DOUBLE))), Optional.empty());
        assertEquals(actualPQL, "SELECT min(fare), max(fare), count(fare), PERCENTILEEST99(fare) FROM tbl group by city top 10");
    }

    @Test(expectedExceptions = PinotException.class)
    public void testBrokerSourceWithoutLimitFails()
    {
        List<ColumnHandle> columnHandles = columnHandles(regionId, city, fare, secondsSinceEpoch);
        TableScanPipeline pipeline = pipeline(scan(pinotTable, columnHandles), filter(pdExpr("fare > 20"), ImmutableList.of("regionid", "city", "fare", "secondssinceepoch"), ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT)));
        PinotQueryGenerator.generateForSingleBrokerRequest(pipeline, Optional.of(ImmutableList.of(regionId, city, fare, secondsSinceEpoch)), Optional.of(new PinotConfig()));
    }

    @Test
    public void testScanOnlyWithBrokerPageSource()
    {
        List<ColumnHandle> columnHandles = columnHandles(regionId, city, fare, secondsSinceEpoch);
        TableScanPipeline pipeline = pipeline(scan(pinotTable, columnHandles));
        List<PinotColumnHandle> outputHandles = getOutputHandlesFromPipeline(pipeline);
        outputHandles.forEach(handle -> assertTrue(handle.getType().equals(REGULAR)));
        PinotConfig pinotConfig = new PinotConfig();
        pinotConfig.setMaxSelectLimitWhenSinglePage(pinotConfig.getLimitLarge());
        PinotQueryGenerator.GeneratedPql generatedPql = PinotQueryGenerator.generateForSingleBrokerRequest(pipeline, Optional.of(getOutputHandlesFromPipeline(pipeline)), Optional.of(pinotConfig));
        assertEquals(pinotTable.getTableName(), generatedPql.getTable());
        assertEquals("SELECT regionId, city, fare, secondsSinceEpoch FROM tbl LIMIT " + pinotConfig.getLimitLarge(), generatedPql.getPql());
    }

    @Test
    public void testBrokerSourceWithSmallLimit()
    {
        ImmutableList<String> columns = ImmutableList.of("regionid", "city", "fare", "secondssinceepoch");
        ImmutableList<Type> columnTypes = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);
        List<ColumnHandle> columnHandles = columnHandles(regionId, city, fare, secondsSinceEpoch);
        TableScanPipeline pipeline = pipeline(scan(pinotTable, columnHandles), filter(pdExpr("fare > 20"), columns, columnTypes), limit(5, false, columns, columnTypes));
        PinotQueryGenerator.GeneratedPql generatedPql = PinotQueryGenerator.generateForSingleBrokerRequest(pipeline, Optional.of(getOutputHandlesFromPipeline(pipeline)), Optional.of(new PinotConfig()));
        assertEquals(pinotTable.getTableName(), generatedPql.getTable());
        assertEquals(0, generatedPql.getNumGroupByClauses());
        assertEquals("SELECT regionId, city, fare, secondsSinceEpoch FROM tbl WHERE (fare > 20) LIMIT 5", generatedPql.getPql());
    }

    private static List<PinotColumnHandle> getOutputHandlesFromPipeline(TableScanPipeline pipeline)
    {
        return pipeline.getOutputColumnHandles().stream().map(x -> (PinotColumnHandle) x).collect(Collectors.toList());
    }
}
