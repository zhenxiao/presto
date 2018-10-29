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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.linkedin.pinot.client.DynamicBrokerSelector;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.NetUtil;
import io.airlift.log.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.linkedin.pinot.common.config.TableNameBuilder.extractRawTableName;

public class PinotClusterInfoFetcher
{
    private static final String APPLICATION_JSON = "application/json";

    private static final String GET_ALL_TABLES_API_TEMPLATE = "http://%s/tables";
    private static final String TABLE_SCHEMA_API_TEMPLATE = "http://%s/tables/%s/schema";
    private static final String ROUTING_TABLE_API_TEMPLATE = "http://%s/debug/routingTable/%s";
    private static final String TIME_BOUNDARY_API_TEMPLATE = "http://%s/debug/timeBoundary/%s";

    private static final String MUTTLEY_RPC_CALLER_HEADER = "Rpc-Caller";
    private static final String MUTTLEY_RPC_CALLER_VALUE = "presto";
    private static final String MUTTLEY_RPC_SERVICE_HEADER = "Rpc-Service";
    private static final String MUTTLEY_RPC_SERVICE_VALUE = "streaming-pinot-controller-%s";

    private static final Logger log = Logger.get(PinotClusterInfoFetcher.class);
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final String controllerUrl;
    private static String clusterEnv;
    private final String zkServers;
    private DynamicBrokerSelector dynamicBrokerSelector;
    private String instanceId = "Presto_pinot_master";

    @Inject
    public PinotClusterInfoFetcher(PinotConfig pinotConfig) throws SocketException, UnknownHostException
    {
        this(pinotConfig.getZkUrl(), pinotConfig.getPinotCluster(), pinotConfig.getControllerUrl(), pinotConfig.getPinotClusterEnv());
    }

    public PinotClusterInfoFetcher(String zkUrl, String pinotCluster, String controllerUrl, String clusterEnv) throws SocketException, UnknownHostException
    {
        log.info("Trying to init PinotClusterInfoFetcher with Zookeeper: %s, PinotCluster %s, ControllerUrl: %s.", zkUrl, pinotCluster, controllerUrl);
        zkServers = zkUrl + "/" + pinotCluster;
        try {
            instanceId = instanceId + "_" + NetUtil.getHostAddress();
        }
        catch (Exception e) {
            // Swallow Exception
            log.error("Failed to get host address.", e);
            throw e;
        }
        this.controllerUrl = controllerUrl;
        PinotClusterInfoFetcher.clusterEnv = clusterEnv;
    }

    public static String sendHttpGet(final String url) throws Exception
    {
        final String rpcService = String.format(MUTTLEY_RPC_SERVICE_VALUE, clusterEnv);
        HttpUriRequest request = RequestBuilder.get(url).setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setHeader(MUTTLEY_RPC_CALLER_HEADER, MUTTLEY_RPC_CALLER_VALUE)
                .setHeader(MUTTLEY_RPC_SERVICE_HEADER, rpcService)
                .build();
        return httpClient.execute(request, getStringResponseHandler());
    }

    private static ResponseHandler<String> getStringResponseHandler()
    {
        return new ResponseHandler<String>()
        {
            @Override
            public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException
            {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    return entity != null ? EntityUtils.toString(entity) : null;
                }
                else {
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            }
        };
    }

    public void close() throws IOException
    {
        httpClient.close();
    }

    private String getControllerUrl()
    {
        return this.controllerUrl;
    }

    @SuppressWarnings("unchecked")
    public List<String> getAllTables() throws Exception
    {
        final String url = String.format(GET_ALL_TABLES_API_TEMPLATE, getControllerUrl());
        String responseBody = sendHttpGet(url);
        Map<String, List<String>> responseMap = new ObjectMapper().readValue(responseBody, Map.class);
        return responseMap.get("tables");
    }

    public Schema getTableSchema(String table) throws Exception
    {
        final String url = String.format(TABLE_SCHEMA_API_TEMPLATE, getControllerUrl(), table);
        String responseBody = sendHttpGet(url);
        return Schema.fromString(responseBody);
    }

    public String getBrokerHost(String table) throws Exception
    {
        if (dynamicBrokerSelector == null) {
            dynamicBrokerSelector = new DynamicBrokerSelector(zkServers);
        }
        return this.dynamicBrokerSelector.selectBroker(table);
    }

    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName) throws Exception
    {
        final Map<String, Map<String, List<String>>> routingTableMap = new HashMap<>();
        final String url = String.format(ROUTING_TABLE_API_TEMPLATE, getBrokerHost(tableName), tableName);
        String responseBody = sendHttpGet(url);
        log.debug("Trying to get routingTable for %s. url: %s", tableName, url);
        JSONObject resp = JSONObject.parseObject(responseBody);
        JSONArray routingTableSnapshots = resp.getJSONArray("routingTableSnapshot");
        for (int i = 0; i < routingTableSnapshots.size(); i++) {
            JSONObject snapshot = routingTableSnapshots.getJSONObject(i);
            String tableNameWithType = snapshot.getString("tableName");
            // Response could contain info for tableName that matches the original table by prefix.
            // e.g. when table name is "table1", response could contain routingTable for "table1_staging"
            if (!tableName.equals(extractRawTableName(tableNameWithType))) {
                log.debug("Ignoring routingTable for %s", tableNameWithType);
                continue;
            }
            JSONArray routingTableEntriesArray = snapshot.getJSONArray("routingTableEntries");
            if (routingTableEntriesArray.size() == 0) {
                log.error("Empty routingTableEntries for %s. RoutingTable: %s", tableName, resp.toString());
                throw new RuntimeException("RoutingTable is empty for " + tableName);
            }
            String routingTableEntries = routingTableEntriesArray.getJSONObject(new Random().nextInt(routingTableEntriesArray.size())).toJSONString();
            Map<String, List<String>> routingTable = new ObjectMapper().readValue(routingTableEntries, Map.class);
            routingTableMap.put(tableNameWithType, routingTable);
        }
        return routingTableMap;
    }

    public Map<String, String> getTimeBoundaryForTable(String table) throws Exception
    {
        final String url = String.format(TIME_BOUNDARY_API_TEMPLATE, getBrokerHost(table), table);
        String responseBody = sendHttpGet(url);
        JSONObject resp = JSONObject.parseObject(responseBody);
        Map<String, String> timeBoundary = new HashMap<>();
        if (resp.containsKey("timeColumnName")) {
            timeBoundary.put("timeColumnName", resp.getString("timeColumnName"));
        }
        if (resp.containsKey("timeColumnValue")) {
            timeBoundary.put("timeColumnValue", resp.getString("timeColumnValue"));
        }
        return timeBoundary;
    }
}
