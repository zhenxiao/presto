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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.pinot.PinotBrokerPageSource.populateFromPqlResults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPinotBrokerPageSource
{
    private static class PqlParsedInfo
    {
        final int numGroupByColumns;
        final int numColumns;
        final int numRows;

        private PqlParsedInfo(int numGroupByColumns, int numColumns, int numRows)
        {
            this.numGroupByColumns = numGroupByColumns;
            this.numColumns = numColumns;
            this.numRows = numRows;
        }

        public static PqlParsedInfo forSelection(int numColumns, int numRows)
        {
            return new PqlParsedInfo(0, numColumns, numRows);
        }

        public static PqlParsedInfo forAggregation(int numGroups, int numAggregates, int numRows)
        {
            return new PqlParsedInfo(numGroups, numGroups + numAggregates, numRows);
        }
    }

    PqlParsedInfo getBasicInfoFromPql(String pqlResponse)
    {
        JSONObject pqlJson = JSONObject.parseObject(pqlResponse);
        if (pqlJson.containsKey("selectionResults")) {
            JSONObject selectionResults = pqlJson.getJSONObject("selectionResults");
            return PqlParsedInfo.forSelection(selectionResults.getJSONArray("columns").size(), selectionResults.getJSONArray("results").size());
        }
        else {
            JSONArray aggregationResults = pqlJson.getJSONArray("aggregationResults");
            int numAggregates = aggregationResults.size();
            Set<List<String>> groups = new HashSet<>();
            int numGroupByColumns = 0;
            int numPureAggregates = 0;
            for (int i = 0; i < numAggregates; ++i) {
                JSONArray groupByResult = aggregationResults.getJSONObject(i).getJSONArray("groupByResult");
                if (groupByResult != null) {
                    for (int j = 0; j < groupByResult.size(); ++j) {
                        JSONArray groupJson = groupByResult.getJSONObject(j).getJSONArray("group");
                        List<String> group = Arrays.asList(groupJson.toArray(new String[groupJson.size()]));
                        groups.add(group);
                        if (numGroupByColumns == 0) {
                            numGroupByColumns = group.size();
                        }
                    }
                }
                else {
                    ++numPureAggregates;
                }
            }
            assertTrue(numPureAggregates == 0 || numPureAggregates == numAggregates, String.format("In pql response %s, got mixed aggregates %d of %d", pqlResponse, numPureAggregates, numAggregates));
            if (numPureAggregates == 0) {
                return PqlParsedInfo.forAggregation(numGroupByColumns, numAggregates, groups.size());
            }
            else {
                return PqlParsedInfo.forAggregation(0, numPureAggregates, 1);
            }
        }
    }

    @DataProvider(name = "pqlResponses")
    public static Object[][] pqlResponsesProvider()
    {
        return new Object[][] {
                {"SELECT count(*), sum(regionId) FROM eats_job_state GROUP BY jobState TOP 1000000",
                        "{\"aggregationResults\":[{\"groupByResult\":[{\"value\":\"10646777\",\"group\":[\"CREATED\"]},{\"value\":\"9441201\",\"group\":[\"ASSIGNED\"]},{\"value\":\"5329962\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"5281666\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"5225839\",\"group\":[\"OFFERED\"]},{\"value\":\"5088568\",\"group\":[\"READY\"]},{\"value\":\"5027369\",\"group\":[\"COMPLETED\"]},{\"value\":\"3677267\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"1559953\",\"group\":[\"SCHEDULED\"]},{\"value\":\"1532913\",\"group\":[\"ACCEPTED\"]},{\"value\":\"1532891\",\"group\":[\"RELEASED\"]},{\"value\":\"531719\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"252977\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"243463\",\"group\":[\"CANCELED\"]},{\"value\":\"211553\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"148548\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"108057\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"47043\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"30832\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"18009\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"16459\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"11086\",\"group\":[\"FAILED\"]},{\"value\":\"9976\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"3094\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"count_star\",\"groupByColumns\":[\"jobState\"]},{\"groupByResult\":[{\"value\":\"3274799599.00000\",\"group\":[\"CREATED\"]},{\"value\":\"2926585674.00000\",\"group\":[\"ASSIGNED\"]},{\"value\":\"1645707788.00000\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"1614715326.00000\",\"group\":[\"OFFERED\"]},{\"value\":\"1608041994.00000\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"1568036720.00000\",\"group\":[\"READY\"]},{\"value\":\"1541977381.00000\",\"group\":[\"COMPLETED\"]},{\"value\":\"1190457213.00000\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"430246171.00000\",\"group\":[\"SCHEDULED\"]},{\"value\":\"422020881.00000\",\"group\":[\"RELEASED\"]},{\"value\":\"421937782.00000\",\"group\":[\"ACCEPTED\"]},{\"value\":\"147557783.00000\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"94882088.00000\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"86447788.00000\",\"group\":[\"CANCELED\"]},{\"value\":\"77505566.00000\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"53955037.00000\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"36026660.00000\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"15306755.00000\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"8811788.00000\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"5301567.00000\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"4855342.00000\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"3113490.00000\",\"group\":[\"FAILED\"]},{\"value\":\"2811789.00000\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"1053944.00000\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"sum_regionId\",\"groupByColumns\":[\"jobState\"]}],\"exceptions\":[],\"numServersQueried\":7,\"numServersResponded\":7,\"numDocsScanned\":55977222,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":111954444,\"totalDocs\":55977222,\"numGroupsLimitReached\":false,\"timeUsedMs\":775,\"segmentStatistics\":[],\"traceInfo\":{}}",
                        ImmutableList.of(VarcharType.VARCHAR, BigintType.BIGINT, BigintType.BIGINT), Optional.empty()},
                {"SELECT count(*) FROM eats_job_state GROUP BY jobState TOP 1000000",
                        "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":55979949,\"numDocsScanned\":55979949,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"groupByResult\":[{\"value\":\"10647363\",\"group\":[\"CREATED\"]},{\"value\":\"9441638\",\"group\":[\"ASSIGNED\"]},{\"value\":\"5330203\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"5281905\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"5226090\",\"group\":[\"OFFERED\"]},{\"value\":\"5088813\",\"group\":[\"READY\"]},{\"value\":\"5027589\",\"group\":[\"COMPLETED\"]},{\"value\":\"3677424\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"1560029\",\"group\":[\"SCHEDULED\"]},{\"value\":\"1533006\",\"group\":[\"ACCEPTED\"]},{\"value\":\"1532980\",\"group\":[\"RELEASED\"]},{\"value\":\"531745\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"252989\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"243477\",\"group\":[\"CANCELED\"]},{\"value\":\"211571\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"148557\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"108062\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"47048\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"30832\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"18009\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"16461\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"11086\",\"group\":[\"FAILED\"]},{\"value\":\"9978\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"3094\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"count_star\",\"groupByColumns\":[\"jobState\"]}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":402,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55979949}",
                        ImmutableList.of(VarcharType.VARCHAR, BigintType.BIGINT), Optional.empty()},
                {"SELECT count(*) FROM eats_job_state",
                        "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":0,\"numDocsScanned\":55981101,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"function\":\"count_star\",\"value\":\"55981101\"}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":7,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55981101}",
                        ImmutableList.of(BigintType.BIGINT), Optional.empty()},
                {"SELECT sum(regionId), count(*) FROM eats_job_state",
                        "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":55981641,\"numDocsScanned\":55981641,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"function\":\"sum_regionId\",\"value\":\"17183585871.00000\"},{\"function\":\"count_star\",\"value\":\"55981641\"}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":549,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55981641}",
                        ImmutableList.of(BigintType.BIGINT, BigintType.BIGINT), Optional.empty()},
                {"SELECT jobState, regionId FROM eats_job_state LIMIT 10",
                        "{\"selectionResults\":{\"columns\":[\"jobState\",\"regionId\"],\"results\":[[\"CREATED\",\"197\"],[\"SUBMITTED_TO_BILLING\",\"227\"],[\"ASSIGNED\",\"188\"],[\"SCHEDULED\",\"1479\"],[\"CANCELED\",\"1708\"],[\"CREATED\",\"134\"],[\"CREATED\",\"12\"],[\"OFFERED\",\"30\"],[\"COMPLETED\",\"215\"],[\"CREATED\",\"7\"]]},\"exceptions\":[],\"numServersQueried\":7,\"numServersResponded\":7,\"numDocsScanned\":380,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":760,\"totalDocs\":55988817,\"numGroupsLimitReached\":false,\"timeUsedMs\":2,\"segmentStatistics\":[],\"traceInfo\":{}}",
                        ImmutableList.of(VarcharType.VARCHAR, BigintType.BIGINT), Optional.empty()},
                {"SELECT shoppingCartUUID, $validUntil, $validFrom, jobState, tenancy, accountUUID, vehicleViewId, $partition, clientUUID, orderJobUUID, productTypeUUID, demandJobUUID, regionId, workflowUUID, jobType, kafkaOffset, productUUID, timestamp, flowType, ts FROM eats_job_state LIMIT 10",
                        "{\"selectionResults\":{\"columns\":[\"shoppingCartUUID\",\"$validUntil\",\"$validFrom\",\"jobState\",\"tenancy\",\"accountUUID\",\"vehicleViewId\",\"$partition\",\"clientUUID\",\"orderJobUUID\",\"productTypeUUID\",\"demandJobUUID\",\"regionId\",\"workflowUUID\",\"jobType\",\"kafkaOffset\",\"productUUID\",\"timestamp\",\"flowType\",\"ts\"],\"results\":[]},\"traceInfo\":{},\"numEntriesScannedPostFilter\":0,\"numDocsScanned\":0,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"exceptions\":[{\"errorCode\":200,\"message\":\"QueryExecutionError:\\njava.lang.NullPointerException\\n\\tat java.lang.Class.forName0(Native Method\\n\\tat\"}],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":3,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":0}",
                        ImmutableList.of(), Optional.of(PinotException.class)}
        };
    }

    @Test(dataProvider = "pqlResponses")
    public void testPopulateFromPql(String pql, String pqlResponse, List<Type> types, Optional<Class<? extends PrestoException>> expectedError)
    {
        PqlParsedInfo pqlParsedInfo = getBasicInfoFromPql(pqlResponse);
        ImmutableList.Builder<BlockBuilder> blockBuilders = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(types);
        for (int i = 0; i < types.size(); ++i) {
            blockBuilders.add(pageBuilder.getBlockBuilder(i));
        }
        Optional<Class<? extends PrestoException>> thrown = Optional.empty();
        int numRows = -1;
        try {
            numRows = populateFromPqlResults(pql, pqlParsedInfo.numGroupByColumns, blockBuilders.build(), types, pqlResponse);
        }
        catch (PrestoException e) {
            thrown = Optional.of(e.getClass());
        }
        assertEquals(thrown, expectedError);
        if (!expectedError.isPresent()) {
            assertEquals(types.size(), pqlParsedInfo.numColumns);
            assertEquals(numRows, pqlParsedInfo.numRows);
        }
    }
}
