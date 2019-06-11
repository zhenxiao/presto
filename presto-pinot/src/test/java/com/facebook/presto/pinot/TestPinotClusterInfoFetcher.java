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

import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class TestPinotClusterInfoFetcher
{
    @Test
    public void testBrokersParsed()
    {
        HttpClient httpClient = new TestingHttpClient((request) -> TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "{\n" +
                "  \"tableName\": \"dummy\",\n" +
                "  \"brokers\": [\n" +
                "    {\n" +
                "      \"tableType\": \"offline\",\n" +
                "      \"instances\": [\n" +
                "        \"Broker_dummy-broker-host1-phx3_6513\",\n" +
                "        \"Broker_dummy-broker-host2-phx3_6513\",\n" +
                "        \"Broker_dummy-broker-host4-phx3_6513\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tableType\": \"realtime\",\n" +
                "      \"instances\": [\n" +
                "        \"Broker_dummy-broker-host1-phx3_6513\",\n" +
                "        \"Broker_dummy-broker-host2-phx3_6513\",\n" +
                "        \"Broker_dummy-broker-host3-phx3_6513\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"server\": [\n" +
                "    {\n" +
                "      \"tableType\": \"offline\",\n" +
                "      \"instances\": [\n" +
                "        \"Server_dummy-server-host8-phx3_7090\",\n" +
                "        \"Server_dummy-server-host9-phx3_7090\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tableType\": \"realtime\",\n" +
                "      \"instances\": [\n" +
                "        \"Server_dummy-server-host7-phx3_7090\",\n" +
                "        \"Server_dummy-server-host4-phx3_7090\",\n" +
                "        \"Server_dummy-server-host5-phx3_7090\",\n" +
                "        \"Server_dummy-server-host6-phx3_7090\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}"));
        PinotConfig pinotConfig = new PinotConfig().setMetadataCacheExpiry(new Duration(0, TimeUnit.MILLISECONDS));
        PinotClusterInfoFetcher pinotClusterInfoFetcher = new PinotClusterInfoFetcher(pinotConfig, new PinotMetrics(), httpClient);
        ImmutableSet<String> brokers = ImmutableSet.copyOf(pinotClusterInfoFetcher.getAllBrokersForTable("dummy"));
        Assert.assertEquals(ImmutableSet.of("dummy-broker-host1-phx3:6513", "dummy-broker-host2-phx3:6513", "dummy-broker-host3-phx3:6513", "dummy-broker-host4-phx3:6513"), brokers);
    }
}
