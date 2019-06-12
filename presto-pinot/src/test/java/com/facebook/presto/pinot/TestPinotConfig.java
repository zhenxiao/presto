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
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotConfig.class)
                        .setExtraHttpHeaders("")
                        .setControllerUrl(null)
                        .setIdleTimeout(new Duration(5, TimeUnit.MINUTES))
                        .setLimitLarge(Long.toString(PinotConfig.DEFAULT_LIMIT_LARGE))
                        .setTopNLarge(Long.toString(PinotConfig.DEFAULT_TOPN_LARGE))
                        .setMaxBacklogPerServer(null)
                        .setMaxConnectionsPerServer(null)
                        .setMinConnectionsPerServer(null)
                        .setThreadPoolSize(null)
                        .setEstimatedSizeInBytesForNonNumericColumn(20)
                        .setConnectionTimeout(new Duration(1, TimeUnit.MINUTES))
                        .setControllerRestService(null)
                        .setServiceHeaderParam("RPC-Service")
                        .setCallerHeaderValue("presto")
                        .setCallerHeaderParam("RPC-Caller")
                        .setMetadataCacheExpiry(new Duration(2, TimeUnit.MINUTES))
                        .setAggregationPushDownEnabled(true)
                        .setFilterPushDownEnabled(true)
                        .setProjectPushDownEnabled(true)
                        .setAllowMultipleAggregations(false)
                        .setMaxSelectLimitWhenSinglePage(50_000)
                        .setScanParallelismEnabled(true)
                        .setRestProxyServiceForQuery(null)
                        .setRestProxyUrl(null)
                        .setForceSingleNodePlan(false)
                        .setNumSegmentsPerSplit(1)
                        .setLimitPushDownEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("extra-http-headers", "k:v")
                .put("controller-rest-service", "pinot-controller-service")
                .put("controller-url", "localhost:15982")
                .put("idle-timeout", "1h")
                .put("limit-large", "100000")
                .put("topn-large", "1000")
                .put("max-backlog-per-server", "15")
                .put("max-connections-per-server", "10")
                .put("min-connections-per-server", "1")
                .put("thread-pool-size", "100")
                .put("estimated-size-in-bytes-for-non-numeric-column", "30")
                .put("connection-timeout", "8m")
                .put("metadata-expiry", "1m")
                .put("caller-header-value", "myCaller")
                .put("caller-header-param", "myParam")
                .put("service-header-param", "myServiceHeader")
                .put("aggregation-pushdown-enabled", "false")
                .put("filter-pushdown-enabled", "false")
                .put("project-pushdown-enabled", "false")
                .put("limit-pushdown-enabled", "false")
                .put("allow-multiple-aggregations", "true")
                .put("max-select-limit-when-single-page", "10000")
                .put("scan-parallelism-enabled", "false")
                .put("rest-proxy-url", "localhost:15982")
                .put("rest-proxy-service-for-query", "upinot-rest-proxy-staging")
                .put("force-single-node-plan", "true")
                .put("num-segments-per-split", "2")
                .build();

        PinotConfig expected = new PinotConfig()
                .setExtraHttpHeaders("k:v")
                .setControllerRestService("pinot-controller-service")
                .setControllerUrl("localhost:15982")
                .setRestProxyUrl("localhost:15982")
                .setIdleTimeout(new Duration(1, TimeUnit.HOURS))
                .setLimitLarge("100000")
                .setTopNLarge("1000")
                .setMaxBacklogPerServer("15")
                .setMaxConnectionsPerServer("10")
                .setMinConnectionsPerServer("1")
                .setThreadPoolSize("100")
                .setEstimatedSizeInBytesForNonNumericColumn(30)
                .setConnectionTimeout(new Duration(8, TimeUnit.MINUTES))
                .setServiceHeaderParam("myServiceHeader")
                .setCallerHeaderValue("myCaller")
                .setCallerHeaderParam("myParam")
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MINUTES))
                .setAggregationPushDownEnabled(false)
                .setFilterPushDownEnabled(false)
                .setProjectPushDownEnabled(false)
                .setAllowMultipleAggregations(true)
                .setMaxSelectLimitWhenSinglePage(10000)
                .setScanParallelismEnabled(false)
                .setRestProxyServiceForQuery("upinot-rest-proxy-staging")
                .setForceSingleNodePlan(true)
                .setNumSegmentsPerSplit(2)
                .setLimitPushDownEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
