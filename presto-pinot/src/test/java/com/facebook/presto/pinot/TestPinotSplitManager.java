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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Aggregation;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.PinotSplit.SplitType.BROKER;
import static com.facebook.presto.pinot.PinotTestUtils.agg;
import static com.facebook.presto.pinot.PinotTestUtils.cols;
import static com.facebook.presto.pinot.PinotTestUtils.columnHandles;
import static com.facebook.presto.pinot.PinotTestUtils.pdExpr;
import static com.facebook.presto.pinot.PinotTestUtils.pipeline;
import static com.facebook.presto.pinot.PinotTestUtils.project;
import static com.facebook.presto.pinot.PinotTestUtils.scan;
import static com.facebook.presto.pinot.PinotTestUtils.types;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPinotSplitManager
{
    private static final PinotConfig pinotConfig = new PinotConfig();
    private static final PinotSplitManager pinotSplitManager = new PinotSplitManager(
            new PinotConnectorId(""),
            new PinotConnection(new MockPinotClusterInfoFetcher(pinotConfig), pinotConfig),
            pinotConfig);
    // Test table and related info
    private static final PinotColumnHandle columnCityId = new PinotColumnHandle("city_id", BIGINT, REGULAR);
    private static final PinotColumnHandle columnCountryName = new PinotColumnHandle("country_name", VARCHAR, REGULAR);
    private static final PinotColumnHandle columnColor = new PinotColumnHandle("color", COLOR, REGULAR);

    public static PinotTableHandle realtimeOnlyTable = new PinotTableHandle("connId", "schema", "realtimeOnly");
    public static PinotTableHandle hybridTable = new PinotTableHandle("connId", "schema", "hybrid");
    public static ColumnHandle regionId = new PinotColumnHandle("regionId", BIGINT, REGULAR);
    public static ColumnHandle city = new PinotColumnHandle("city", VARCHAR, REGULAR);
    public static ColumnHandle fare = new PinotColumnHandle("fare", DOUBLE, REGULAR);
    public static ColumnHandle secondsSinceEpoch = new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);

    @Test
    public void testGetSplitsWithNoTupleDomainSingleSplit()
    {
        Aggregation min = new Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        Aggregation percentile = new Aggregation(ImmutableList.of("fare", "percentile"), "approx_percentile", "fare_percentile", DOUBLE);

        TableScanPipeline scanPipeline = pipeline(
                scan(realtimeOnlyTable, columnHandles(fare)),
                project(ImmutableList.of(pdExpr("fare"), pdExpr("99")), cols("fare", "percentile"), types(DOUBLE, DOUBLE)),
                agg(ImmutableList.of(min, percentile), false));

        PinotSplit pinotSplit = getOnlyElement(getSplitsHelper(realtimeOnlyTable, scanPipeline, Optional.empty()));

        assertEquals(pinotSplit.getSplitType(), BROKER);
        assertTrue(pinotSplit.getPipeline().isPresent());
        assertEquals(pinotSplit.getPipeline().get(), scanPipeline);
    }

    @Test
    public void testGetSplitsWithNoTupleDomainMultiSplit()
    {
        TableScanPipeline scanPipeline = pipeline(scan(realtimeOnlyTable, columnHandles(regionId, city, fare, secondsSinceEpoch)));
        List<PinotSplit> splits = getSplitsHelper(realtimeOnlyTable, scanPipeline, Optional.empty());
        assertEquals(splits.size(), 4); // expects 4 splits as there are 2 servers holding 4 segments total

        scanPipeline = pipeline(scan(hybridTable, columnHandles(regionId, city, fare, secondsSinceEpoch)));
        splits = getSplitsHelper(hybridTable, scanPipeline, Optional.empty());
        assertEquals(splits.size(), 8); // expects 8 splits as there are 4 servers holding 4 segments total (split between offline and online)
    }

    @Test
    public void testGetSplitsWithTupleDomain()
    {
        Domain regionIdDomain = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 10L)), false);

        Domain cityDomain = Domain.create(ValueSet.ofRanges(
                Range.equal(VARCHAR, Slices.utf8Slice("Campbell")),
                Range.equal(VARCHAR, Slices.utf8Slice("Union City"))), false);

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        domainMap.put(regionId, regionIdDomain);
        domainMap.put(city, cityDomain);
        TupleDomain<ColumnHandle> constraintSummary = TupleDomain.withColumnDomains(domainMap);

        TableScanPipeline scanPipeline = pipeline(scan(realtimeOnlyTable, columnHandles(TestPinotSplitManager.regionId, TestPinotSplitManager.city, fare, secondsSinceEpoch)));
        List<PinotSplit> splits = getSplitsHelper(realtimeOnlyTable, scanPipeline, Optional.of(constraintSummary));
        assertEquals(splits.size(), 4); // expects 4 splits as there are 2 servers holding 4 segments total
        for (PinotSplit split : splits) {
            // make sure all splits contain the filter that comes from TupleDomain
            assertTrue(split.getPql().get().contains("((regionId < 10) AND ((city = 'Campbell') OR (city = 'Union City'))"));
        }

        scanPipeline = pipeline(scan(hybridTable, columnHandles(TestPinotSplitManager.regionId, TestPinotSplitManager.city, fare, secondsSinceEpoch)));
        splits = getSplitsHelper(hybridTable, scanPipeline, Optional.of(constraintSummary));
        assertEquals(splits.size(), 8); // expects 8 splits as there are 4 servers holding 4 segments total (split between offline and online)
        for (PinotSplit split : splits) {
            // make sure all splits contain the filter that comes from TupleDomain and the time filter
            String pql = split.getPql().get();
            assertTrue(pql.contains("((regionId < 10) AND ((city = 'Campbell') OR (city = 'Union City'))"), pql);
            assertTrue(pql.contains("secondsSinceEpoch >=") || pql.contains("secondsSinceEpoch <"), pql);
        }
    }

    @Test
    public void testGetSplitsWithTupleDomainScanOutputColumnAliases()
    {
        Domain regionIdDomain = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 10L)), false);

        Domain cityDomain = Domain.create(ValueSet.ofRanges(
                Range.equal(VARCHAR, Slices.utf8Slice("Campbell")),
                Range.equal(VARCHAR, Slices.utf8Slice("Union City"))), false);

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        domainMap.put(regionId, regionIdDomain);
        domainMap.put(city, cityDomain);
        TupleDomain<ColumnHandle> constraintSummary = TupleDomain.withColumnDomains(domainMap);

        TableScanPipeline scanPipeline = pipeline(
                scan(realtimeOnlyTable,
                        columnHandles(TestPinotSplitManager.regionId, TestPinotSplitManager.city, fare, secondsSinceEpoch),
                        asList("region_id_123", "city_124", "fare_125", "secondssinceepoch")));
        List<PinotSplit> splits = getSplitsHelper(realtimeOnlyTable, scanPipeline, Optional.of(constraintSummary));
        assertEquals(splits.size(), 4); // expects 4 splits as there are 2 servers holding 4 segments total
        for (PinotSplit split : splits) {
            // make sure all splits contain the filter that comes from TupleDomain
            assertTrue(split.getPql().get().contains("((regionId < 10) AND ((city = 'Campbell') OR (city = 'Union City'))"));
        }

        scanPipeline = pipeline(
                scan(hybridTable,
                        columnHandles(TestPinotSplitManager.regionId, TestPinotSplitManager.city, fare, secondsSinceEpoch),
                        asList("region_id_123", "city_124", "fare_125", "secondssinceepoch")));

        splits = getSplitsHelper(hybridTable, scanPipeline, Optional.of(constraintSummary));
        assertEquals(splits.size(), 8); // expects 8 splits as there are 4 servers holding 4 segments total (split between offline and online)
        for (PinotSplit split : splits) {
            // make sure all splits contain the filter that comes from TupleDomain and the time filter
            String pql = split.getPql().get();
            assertTrue(pql.contains("((regionId < 10) AND ((city = 'Campbell') OR (city = 'Union City'))"), pql);
            assertTrue(pql.contains("secondsSinceEpoch >=") || pql.contains("secondsSinceEpoch <"), pql);
        }
    }

    private List<PinotSplit> getSplitsHelper(PinotTableHandle pinotTable, TableScanPipeline scanPipeline, Optional<TupleDomain<ColumnHandle>> constraint)
    {
        PinotTableLayoutHandle pinotTableLayout = new PinotTableLayoutHandle(pinotTable, constraint, Optional.of(scanPipeline));

        ConnectorSplitSource splitSource = pinotSplitManager.getSplits(null, null, pinotTableLayout, null);
        List<PinotSplit> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits().stream().map(s -> (PinotSplit) s).collect(toList()));
        }

        return splits;
    }

    @Test
    public void testSingleValueRanges()
    {
        Domain domain = com.facebook.presto.spi.predicate.Domain.multipleValues(BIGINT, new ArrayList<>(asList(1L, 10L)));

        assertEquals(pinotSplitManager.getColumnPredicate(domain, columnCityId, "city_id").get().toString(), "((city_id = 1) OR (city_id = 10))");
        assertEquals(pinotSplitManager.getColumnPredicate(domain, columnCityId, "city_id_123").get().toString(), "((city_id_123 = 1) OR (city_id_123 = 10))");
    }

    @Test
    public void testRangeValues()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L))), false);

        String expectedFilter = "((1 < city_id) AND (city_id < 10))";
        assertEquals(pinotSplitManager.getColumnPredicate(domain, columnCityId, columnCityId.getColumnName()).get().toString(), expectedFilter);
    }

    @Test
    public void testOneSideRanges()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.lessThanOrEqual(BIGINT, 10L)), false);

        String expectedFilter = "(city_id <= 10)";
        assertEquals(pinotSplitManager.getColumnPredicate(domain, columnCityId, columnCityId.getColumnName()).get().toString(), expectedFilter);
    }

    @Test
    public void testMultipleRanges()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.equal(BIGINT, 20L),
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L)),
                Range.greaterThan(BIGINT, 12L).intersect(Range.lessThan(BIGINT, 18L))), false);

        String expectedFilter = "(((1 < city_id) AND (city_id < 10)) OR (((12 < city_id) AND (city_id < 18)) OR (city_id = 20)))";
        assertEquals(pinotSplitManager.getColumnPredicate(domain, columnCityId, columnCityId.getColumnName()).get().toString(), expectedFilter);
    }

    @Test
    public void testMultipleColumns()
    {
        Domain domain1 = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 10L)), false);

        Domain domain2 = Domain.create(ValueSet.ofRanges(
                Range.equal(VARCHAR, Slices.utf8Slice("cn")),
                Range.equal(VARCHAR, Slices.utf8Slice("us"))), false);

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        domainMap.put(columnCityId, domain1);
        domainMap.put(columnCountryName, domain2);
        TupleDomain<ColumnHandle> constraintSummary = TupleDomain.withColumnDomains(domainMap);

        Map<ColumnHandle, String> columnAliasMap = ImmutableMap.of(
                columnCityId, columnCityId.getColumnName(),
                columnCountryName, columnCountryName.getColumnName());

        String expectedFilter = "((city_id < 10) AND ((country_name = 'cn') OR (country_name = 'us')))";
        assertEquals(pinotSplitManager.getPredicate(constraintSummary, columnAliasMap).toString(), expectedFilter);
    }

    @Test
    public void testNegativeDiscreteValues()
    {
        HashSet<EquatableValueSet.ValueEntry> set = new HashSet<>();
        set.add(EquatableValueSet.ValueEntry.create(COLOR, 1L));
        set.add(EquatableValueSet.ValueEntry.create(COLOR, 2L));
        Domain domain1 = Domain.create(new EquatableValueSet(COLOR, false, set), false);
        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        domainMap.put(columnColor, domain1);

        Map<ColumnHandle, String> columnAliasMap = ImmutableMap.of(columnColor, columnColor.getColumnName());

        TupleDomain<ColumnHandle> constraintSummary = TupleDomain.withColumnDomains(domainMap);

        String expectedFilter = "color NOT IN (1, 2)";
        assertEquals(pinotSplitManager.getPredicate(constraintSummary, columnAliasMap).toString(), expectedFilter);
    }

    /**
     * Test NOT predicate. Note that types currently supported by Pinot are all orderable,
     * so discrete values would appear as single values in the ranges
     * <p>
     * In the test below, the original predicate is WHERE city_id NOT IN (1, 10).
     * - The TupleDomain passed to Pinot is the allowed ranges, and instead of discrete values
     * - So the final translated predicate would be the union of (-Infinity, 1), (1, 10), (10, Infinity)
     * - It might not look as clean as the original predicate, but is still accurate
     */
    @Test
    public void testNotPredicateInRanges()
    {
        Domain domain1 = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 1L),
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L)),
                Range.greaterThan(BIGINT, 10L)), false);

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        domainMap.put(columnCityId, domain1);
        TupleDomain<ColumnHandle> constraintSummary = TupleDomain.withColumnDomains(domainMap);
        Map<ColumnHandle, String> columnAliasMap = ImmutableMap.of(columnCityId, columnCityId.getColumnName());

        String expectedFilter = "((city_id < 1) OR (((1 < city_id) AND (city_id < 10)) OR (10 < city_id)))";
        assertEquals(pinotSplitManager.getPredicate(constraintSummary, columnAliasMap).toString(), expectedFilter);
    }

    @Test
    public void testEmptyDomain()
    {
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, new ArrayList<>());
        Domain domain = Domain.create(sortedRangeSet, false);

        assertEquals(pinotSplitManager.getColumnPredicate(domain, columnCityId, columnCityId.getColumnName()), Optional.empty());
    }
}
