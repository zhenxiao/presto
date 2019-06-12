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

import com.facebook.presto.pinot.MockPinotClusterInfoFetcher;
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotConnection;
import com.facebook.presto.pinot.PinotConnectorId;
import com.facebook.presto.pinot.PinotMetadata;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.PinotTestUtils.agg;
import static com.facebook.presto.pinot.PinotTestUtils.cols;
import static com.facebook.presto.pinot.PinotTestUtils.columnHandles;
import static com.facebook.presto.pinot.PinotTestUtils.limit;
import static com.facebook.presto.pinot.PinotTestUtils.pdExpr;
import static com.facebook.presto.pinot.PinotTestUtils.pipeline;
import static com.facebook.presto.pinot.PinotTestUtils.project;
import static com.facebook.presto.pinot.PinotTestUtils.scan;
import static com.facebook.presto.pinot.PinotTestUtils.types;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPinotMetadata
{
    // Test table and related info
    private static PinotConnectorId pinotConnectorId = new PinotConnectorId("connId");
    private static PinotTableHandle pinotTable = new PinotTableHandle("connId", "schema", "tbl");
    private static PinotColumnHandle regionId = new PinotColumnHandle("regionId", BIGINT, REGULAR);
    private static PinotColumnHandle city = new PinotColumnHandle("city", VARCHAR, REGULAR);
    private static PinotColumnHandle fare = new PinotColumnHandle("fare", DOUBLE, REGULAR);
    private static PinotColumnHandle secondsSinceEpoch = new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);

    private static Optional<TableScanPipeline> pushLimitIntoScan(boolean scanParallelismEnabled, ConnectorTableHandle connectorTableHandle, TableScanPipeline currentPipeline, LimitPipelineNode limit)
    {
        PinotConfig pinotConfig = new PinotConfig().setScanParallelismEnabled(scanParallelismEnabled);
        PinotMetadata metadata = new PinotMetadata(pinotConnectorId, new PinotConnection(new MockPinotClusterInfoFetcher(pinotConfig), pinotConfig), pinotConfig);
        ConnectorSession session = new TestingConnectorSession(new PinotSessionProperties(pinotConfig).getSessionProperties());
        return metadata.pushLimitIntoScan(session, connectorTableHandle, currentPipeline, limit);
    }

    @Test
    public void testFinalLimitWithScan()
    {
        List<String> cols = cols("regionid", "fare", "percentile");
        List<Type> columnTypes = types(BIGINT, DOUBLE, DOUBLE);
        TableScanPipeline existingPipeline = pipeline(
                scan(pinotTable, columnHandles(regionId, fare)),
                project(ImmutableList.of(pdExpr("regionid"), pdExpr("fare"), pdExpr("2")), cols, columnTypes));
        Optional<TableScanPipeline> resultingPipelineWithDefault = pushLimitIntoScan(true, pinotTable, existingPipeline, limit(5, false, cols, columnTypes));
        assertFalse(resultingPipelineWithDefault.isPresent());
        Optional<TableScanPipeline> resultingPipelineWithNoScanParallelism = pushLimitIntoScan(false, pinotTable, existingPipeline, limit(5, false, cols, columnTypes));
        assertTrue(resultingPipelineWithNoScanParallelism.isPresent());
    }

    @Test
    public void testFinalLimitPushedDownWithAggregation()
    {
        AggregationPipelineNode.Aggregation min = new AggregationPipelineNode.Aggregation(ImmutableList.of("fare"), "min", "fare_min", DOUBLE);
        AggregationPipelineNode.GroupByColumn groupByCityId = new AggregationPipelineNode.GroupByColumn("city", "city", VARCHAR);

        TableScanPipeline existingPipeline = pipeline(
                scan(pinotTable, columnHandles(city, fare)),
                project(ImmutableList.of(pdExpr("city"), pdExpr("fare"), pdExpr("99")),
                        cols("city", "fare", "percentile"), types(VARCHAR, DOUBLE, DOUBLE)),
                agg(ImmutableList.of(min, groupByCityId), false));
        Optional<TableScanPipeline> resultingPipelineWithDefault = pushLimitIntoScan(true, pinotTable, existingPipeline, limit(5, false, cols("fare_min", "city"), types(DOUBLE, VARCHAR)));
        assertTrue(resultingPipelineWithDefault.isPresent());
        Optional<TableScanPipeline> resultingPipelineWithNoScanParallelism = pushLimitIntoScan(false, pinotTable, existingPipeline, limit(5, false, cols("fare_min", "city"), types(DOUBLE, VARCHAR)));
        assertTrue(resultingPipelineWithNoScanParallelism.isPresent());
    }
}
