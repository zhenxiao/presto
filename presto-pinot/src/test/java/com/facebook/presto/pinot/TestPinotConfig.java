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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPinotConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(PinotConfig.class).setZkUrl(null).setPinotCluster(null).setPinotClusterEnv(null).setControllerUrl(null).setIdleTimeout(new Duration(5, TimeUnit.MINUTES)).setLimitAll(null).setLimitLarge(null).setLimitMedium(null).setMaxBacklogPerServer(null).setMaxConnectionsPerServer(null).setMinConnectionsPerServer(null).setThreadPoolSize(null).setEstimatedSizeInBytesForNonNumericColumn(20).setIsAggregationPushdownEnabled("true").setConnectionTimeout(new Duration(1, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("zk-uri", "localhost:2181").put("pinot-cluster", "upinot").put("pinot-cluster-env", "adhoc2").put("controller-url", "localhost:15982").put("idle-timeout", "1h").put("limit-all", "2147483646").put("limit-large", "10000000").put("limit-medium", "100000").put("max-backlog-per-server", "15").put("max-connections-per-server", "10").put("min-connections-per-server", "1").put("thread-pool-size", "100").put("estimated-size-in-bytes-for-non-numeric-column", "30").put("aggregation-pushdown.enabled", "false").put("connection-timeout", "8m").build();

        PinotConfig expected = new PinotConfig().setZkUrl("localhost:2181").setPinotCluster("upinot").setPinotClusterEnv("adhoc2").setControllerUrl("localhost:15982").setIdleTimeout(new Duration(1, TimeUnit.HOURS)).setLimitAll("2147483646").setLimitLarge("10000000").setLimitMedium("100000").setMaxBacklogPerServer("15").setMaxConnectionsPerServer("10").setMinConnectionsPerServer("1").setThreadPoolSize("100").setEstimatedSizeInBytesForNonNumericColumn(30).setIsAggregationPushdownEnabled("false").setConnectionTimeout(new Duration(8, TimeUnit.MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
