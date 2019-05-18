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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static io.airlift.http.client.ResponseHandlerUtils.propagate;

/**
 * This class communicates with the rta-ums (muttley)/rtaums (udeploy) service. It's api is available here:
 * https://rtaums.uberinternal.com/api/
 */
public class RTAMSClient
{
    private static final String MUTTLEY_RPC_CALLER_HEADER = "Rpc-Caller";
    private static final String MUTTLEY_RPC_CALLER_VALUE = "presto";
    private static final String MUTTLEY_RPC_SERVICE_HEADER = "Rpc-Service";
    private static final String DEFAULT_MUTTLEY_CLUSTER = "rtaums-staging";
    private static final String MUTTLEY_RPC_SERVICE_VALUE = "rtaums";
    private static final String APPLICATION_JSON = "application/json";
    private static final ObjectMapper mapper = new ObjectMapper();

    private static ResponseHandler<List<String>, IOException> arrayResponseHandler = new ResponseHandler<List<String>, IOException>()
    {
        @Override
        public List<String> handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public List<String> handle(Request request, Response response)
                throws IOException
        {
            int status = response.getStatusCode();
            if (status >= 200 && status < 300) {
                byte[] entityBytes = ByteStreams.toByteArray(response.getInputStream());
                String[] namespaces = mapper.readValue(entityBytes, String[].class);
                return Arrays.asList(namespaces);
            }
            else {
                throw new RuntimeException("Unexpected response status: " + status + ", response: " + response);
            }
        }
    };

    private static ResponseHandler<RTADefinition, IOException> definitionHandler = new ResponseHandler<RTADefinition, IOException>()
    {
        @Override
        public RTADefinition handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public RTADefinition handle(Request request, Response response)
                throws IOException
        {
            int status = response.getStatusCode();
            if (status >= 200 && status < 300) {
                byte[] entityBytes = ByteStreams.toByteArray(response.getInputStream());
                return mapper.readValue(entityBytes, RTADefinition.class);
            }
            else {
                throw new RuntimeException("Unexpected response status for deployment: " + status + ", response: " + response);
            }
        }
    };

    private static ResponseHandler<List<RTADeployment>, IOException> deploymentHandler = new ResponseHandler<List<RTADeployment>, IOException>()
    {
        @Override
        public List<RTADeployment> handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public List<RTADeployment> handle(Request request, Response response)
                throws IOException
        {
            int status = response.getStatusCode();
            if (status >= 200 && status < 300) {
                byte[] entityBytes = ByteStreams.toByteArray(response.getInputStream());
                return mapper.readValue(entityBytes, new TypeReference<List<RTADeployment>>() {});
            }
            else {
                throw new RuntimeException("Unexpected response status for deployment: " + status + ", response: " + response);
            }
        }
    };

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private HttpClient httpClient;
    private final String muttleyService;

    RTAMSClient(HttpClient httpClient, String muttleyRpcServiceValue)
    {
        muttleyService = muttleyRpcServiceValue;
        this.httpClient = httpClient;
    }

    @Inject
    RTAMSClient(@ForRTAMS HttpClient httpClient)
    {
        this(httpClient, DEFAULT_MUTTLEY_CLUSTER);
    }

    Request.Builder getBaseRequest()
    {
        return Request.builder().prepareGet().setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setHeader(MUTTLEY_RPC_CALLER_HEADER, MUTTLEY_RPC_CALLER_VALUE)
                .setHeader(MUTTLEY_RPC_SERVICE_HEADER, muttleyService);
    }

    public List<String> getNamespaces()
            throws IOException
    {
        return httpClient.execute(getBaseRequest().setUri(URI.create(RTAMSEndpoints.getNamespaces())).build(), arrayResponseHandler);
    }

    public List<String> getTables(String namespace)
            throws IOException
    {
        return httpClient.execute(getBaseRequest().setUri(URI.create(RTAMSEndpoints.getTablesFromNamespace(namespace))).build(), arrayResponseHandler);
    }

    public RTADefinition getDefinition(final String namespace, final String tableName)
            throws IOException
    {
        return httpClient.execute(getBaseRequest().setUri(URI.create(RTAMSEndpoints.getTableSchema(namespace, tableName))).build(), definitionHandler);
    }

    public List<RTADeployment> getDeployments(final String namespace, final String tableName)
            throws IOException
    {
        List<RTADeployment> deployments = httpClient.execute(getBaseRequest().setUri(URI.create(RTAMSEndpoints.getDeployment(namespace, tableName))).build(), deploymentHandler);
        return deployments;
    }
}
