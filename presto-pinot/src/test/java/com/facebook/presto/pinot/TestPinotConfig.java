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
import org.testng.annotations.Test;

import java.util.Map;

public class TestPinotConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(PinotConfig.class).setZkUrl(null).setPinotCluster(null).setPinotClusterEnv(null).setControllerUrl(null).setIdleTimeoutMs(null).setLimitAll(null).setLimitLarge(null).setLimitMedium(null).setMaxBacklogPerServer(null).setMaxConnectionsPerServer(null).setMinConnectionsPerServer(null).setThreadPoolSize(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("zk-uri", "localhost:2181").put("pinot-cluster", "upinot").put("pinot-cluster-env", "adhoc2").put("controller-url", "localhost:15982").put("idle-timeout-ms", "20000").put("limit-all", "2147483646").put("limit-large", "10000000").put("limit-medium", "100000").put("max-backlog-per-server", "15").put("max-connections-per-server", "10").put("min-connections-per-server", "1").put("thread-pool-size", "100").build();

        PinotConfig expected = new PinotConfig().setZkUrl("localhost:2181").setPinotCluster("upinot").setPinotClusterEnv("adhoc2").setControllerUrl("localhost:15982").setIdleTimeoutMs("20000").setLimitAll("2147483646").setLimitLarge("10000000").setLimitMedium("100000").setMaxBacklogPerServer("15").setMaxConnectionsPerServer("10").setMinConnectionsPerServer("1").setThreadPoolSize("100");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
