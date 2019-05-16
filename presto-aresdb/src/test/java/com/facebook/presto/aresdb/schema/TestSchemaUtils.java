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

package com.facebook.presto.aresdb.schema;

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public final class TestSchemaUtils
{
    private TestSchemaUtils()
    {
    }

    static RTADefinition getDefinition()
            throws IOException
    {
        String resource = "/eats_order_schema.json";
        RTAMSClient rtamsClient = getMockClient(resource, 200);
        return rtamsClient.getDefinition("rta", "rta_eats_order");
    }

    static List<RTADeployment> getDeployments()
            throws IOException
    {
        String resource = "/eats_order_deployments.json";
        RTAMSClient rtamsClient = getMockClient(resource, 200);
        return rtamsClient.getDeployments("rta", "rta_eats_order");
    }

    static List<String> getNamespaces()
            throws IOException
    {
        String resource = "/namespaces.json";
        RTAMSClient rtamsClient = getMockClient(resource, 200);
        return rtamsClient.getNamespaces();
    }

    static List<String> getTables()
            throws IOException
    {
        String resource = "/rta_tables.json";
        RTAMSClient rtamsClient = getMockClient(resource, 200);
        return rtamsClient.getTables("rta");
    }

    static RTAMSClient getMockClient(String resource, int returnCode)
    {
        InputStream deploymentOutput = TestRTAClient.class.getResourceAsStream(resource);
        HttpClient client = new TestingHttpClient(request -> {
            HttpStatus status = HttpStatus.fromStatusCode(returnCode);
            com.google.common.collect.ImmutableListMultimap.Builder<String, String> headers = ImmutableListMultimap.builder();
            return new TestingResponse(status, headers.build(), deploymentOutput);
        });
        client = Mockito.spy(client);
        RTAMSClient rtamsClient = new RTAMSClient(client);
        return rtamsClient;
    }
}
