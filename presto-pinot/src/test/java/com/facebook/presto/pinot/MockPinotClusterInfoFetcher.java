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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.testing.TestingHttpClient;

import java.util.List;
import java.util.Map;

public class MockPinotClusterInfoFetcher
        extends PinotClusterInfoFetcher
{
    public MockPinotClusterInfoFetcher(PinotConfig pinotConfig)
    {
        super(pinotConfig, new PinotMetrics(), new TestingHttpClient(request -> null));
    }

    @Override
    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
    {
        ImmutableMap.Builder<String, Map<String, List<String>>> routingTable = ImmutableMap.builder();

        if (TestPinotSplitManager.realtimeOnlyTable.getTableName().equalsIgnoreCase(tableName) || TestPinotSplitManager.hybridTable.getTableName().equalsIgnoreCase(tableName)) {
            routingTable.put(tableName + "_REALTIME", ImmutableMap.of(
                    "server1", ImmutableList.of("segment11", "segment12"),
                    "server2", ImmutableList.of("segment21", "segment22")));
        }

        if (TestPinotSplitManager.hybridTable.getTableName().equalsIgnoreCase(tableName)) {
            routingTable.put(tableName + "_OFFLINE", ImmutableMap.of(
                    "server3", ImmutableList.of("segment31", "segment32"),
                    "server4", ImmutableList.of("segment41", "segment42")));
        }

        return routingTable.build();
    }

    @Override
    public Map<String, String> getTimeBoundaryForTable(String table)
    {
        if (TestPinotSplitManager.hybridTable.getTableName().equalsIgnoreCase(table)) {
            return ImmutableMap.of(
                    "timeColumnName", "secondsSinceEpoch",
                    "timeColumnValue", "4562345");
        }

        return ImmutableMap.of();
    }
}
