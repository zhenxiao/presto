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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.log.Logger;
import org.apache.http.HttpHeaders;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.linkedin.pinot.common.config.TableNameBuilder.extractRawTableName;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

public class PinotClusterInfoFetcher
{
    private static final Logger log = Logger.get(PinotClusterInfoFetcher.class);
    private static final String APPLICATION_JSON = "application/json";

    private static final String GET_ALL_TABLES_API_TEMPLATE = "http://%s/tables";
    private static final String TABLE_SCHEMA_API_TEMPLATE = "http://%s/tables/%s/schema";
    private static final String ROUTING_TABLE_API_TEMPLATE = "http://%s/debug/routingTable/%s";
    private static final String TIME_BOUNDARY_API_TEMPLATE = "http://%s/debug/timeBoundary/%s";

    private final HttpClient httpClient;
    private final PinotConfig pinotConfig;

    @GuardedBy("this")
    private DynamicBrokerSelector dynamicBrokerSelector;

    @Inject
    public PinotClusterInfoFetcher(PinotConfig pinotConfig, @ForPinot HttpClient httpClient)
    {
        this.pinotConfig = pinotConfig;
        this.httpClient = httpClient;
    }

    private String getZkServers()
    {
        return pinotConfig.getZkUrl() + "/" + pinotConfig.getPinotCluster();
    }

    public String doHttpActionWithHeaders(Request.Builder requestBuilder, String rpcService)
    {
        requestBuilder = requestBuilder
                .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setHeader(pinotConfig.getCallerHeaderParam(), pinotConfig.getCallerHeaderValue())
                .setHeader(pinotConfig.getServiceHeaderParam(), rpcService);
        pinotConfig.getExtraHttpHeaders().forEach(requestBuilder::setHeader);
        Request request = requestBuilder.build();
        StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());

        if (PinotUtils.isValidPinotHttpResponseCode(response.getStatusCode())) {
            return response.getBody();
        }
        else {
            throw new PinotException(PinotErrorCode.PINOT_HTTP_ERROR,
                    Optional.empty(),
                    "Unexpected response status: " + response.getStatusCode() + " for request " + request);
        }
    }

    public String sendHttpGet(final String url, boolean forBroker)
    {
        return doHttpActionWithHeaders(Request.builder().prepareGet().setUri(URI.create(url)),
                forBroker ? pinotConfig.getBrokerRestService() : pinotConfig.getControllerRestService());
    }

    private String getControllerUrl()
    {
        return pinotConfig.getControllerUrl();
    }

    @SuppressWarnings("unchecked")
    public List<String> getAllTables()
            throws Exception
    {
        final String url = String.format(GET_ALL_TABLES_API_TEMPLATE, getControllerUrl());
        String responseBody = sendHttpGet(url, true);
        Map<String, List<String>> responseMap = new ObjectMapper().readValue(responseBody, Map.class);
        return responseMap.get("tables");
    }

    public Schema getTableSchema(String table)
            throws Exception
    {
        final String url = String.format(TABLE_SCHEMA_API_TEMPLATE, getControllerUrl(), table);
        String responseBody = sendHttpGet(url, true);
        return Schema.fromString(responseBody);
    }

    public String getBrokerHost(String table)
    {
        synchronized (this) {
            if (dynamicBrokerSelector == null) {
                // TODO: DynamicBrokerSelector actually waits for upto a minute to connect to the ZKs
                dynamicBrokerSelector = new DynamicBrokerSelector(getZkServers());
            }
        }
        return dynamicBrokerSelector.selectBroker(table);
    }

    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
            throws Exception
    {
        final Map<String, Map<String, List<String>>> routingTableMap = new HashMap<>();
        final String url = String.format(ROUTING_TABLE_API_TEMPLATE, getBrokerHost(tableName), tableName);
        String responseBody = sendHttpGet(url, false);
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("pinotConfig", pinotConfig)
                .toString();
    }

    public Map<String, String> getTimeBoundaryForTable(String table)
            throws Exception
    {
        final String url = String.format(TIME_BOUNDARY_API_TEMPLATE, getBrokerHost(table), table);
        String responseBody = sendHttpGet(url, false);
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
