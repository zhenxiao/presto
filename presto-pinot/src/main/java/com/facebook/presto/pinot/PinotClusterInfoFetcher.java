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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.linkedin.pinot.common.data.Schema;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.log.Logger;
import org.apache.http.HttpHeaders;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.linkedin.pinot.common.config.TableNameBuilder.extractRawTableName;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

public class PinotClusterInfoFetcher
{
    private static final Logger log = Logger.get(PinotClusterInfoFetcher.class);
    private static final String APPLICATION_JSON = "application/json";
    private static final Pattern BROKER_PATTERN = Pattern.compile("Broker_(.*)_(\\d+)");

    private static final String GET_ALL_TABLES_API_TEMPLATE = "tables";
    private static final String TABLE_INSTANCES_API_TEMPLATE = "tables/%s/instances";
    private static final String TABLE_SCHEMA_API_TEMPLATE = "tables/%s/schema";
    private static final String ROUTING_TABLE_API_TEMPLATE = "debug/routingTable/%s";
    private static final String TIME_BOUNDARY_API_TEMPLATE = "debug/timeBoundary/%s";

    private final PinotConfig pinotConfig;
    private final PinotMetrics pinotMetrics;
    private final HttpClient httpClient;

    private final Ticker ticker = Ticker.systemTicker();
    private final LoadingCache<String, List<String>> brokersForTableCache;

    @Inject
    public PinotClusterInfoFetcher(PinotConfig pinotConfig, PinotMetrics pinotMetrics, @ForPinot HttpClient httpClient)
    {
        final long cacheExpiryMs = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);

        this.pinotConfig = pinotConfig;
        this.pinotMetrics = pinotMetrics;
        this.httpClient = httpClient;
        this.brokersForTableCache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS).build((CacheLoader.from(this::getAllBrokersForTable)));
    }

    public String doHttpActionWithHeaders(Request.Builder requestBuilder, Optional<String> requestBody, Optional<String> rpcService)
    {
        requestBuilder = requestBuilder
                .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON);
        if (rpcService.isPresent()) {
            requestBuilder
                    .setHeader(pinotConfig.getCallerHeaderParam(), pinotConfig.getCallerHeaderValue())
                    .setHeader(pinotConfig.getServiceHeaderParam(), rpcService.get());
        }
        if (requestBody.isPresent()) {
            requestBuilder.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(requestBody.get(), StandardCharsets.UTF_8));
        }
        pinotConfig.getExtraHttpHeaders().forEach(requestBuilder::setHeader);
        Request request = requestBuilder.build();

        long startTime = ticker.read();
        long duration;
        StringResponseHandler.StringResponse response;
        try {
            response = httpClient.execute(request, createStringResponseHandler());
        }
        finally {
            duration = ticker.read() - startTime;
        }
        pinotMetrics.monitorRequest(request, response, duration, TimeUnit.NANOSECONDS);
        String responseBody = response.getBody();
        if (PinotUtils.isValidPinotHttpResponseCode(response.getStatusCode())) {
            return responseBody;
        }
        else {
            throw new PinotException(PinotErrorCode.PINOT_HTTP_ERROR,
                    Optional.empty(),
                    String.format("Unexpected response status: %d for request %s to url %s, with headers %s, full response %s", response.getStatusCode(), requestBody.orElse(""), request.getUri(), request.getHeaders(), responseBody));
        }
    }

    public String sendHttpGetToController(String path)
    {
        return doHttpActionWithHeaders(
                Request.builder().prepareGet().setUri(URI.create(String.format("http://%s/%s", getControllerUrl(), path))),
                Optional.empty(),
                Optional.ofNullable(pinotConfig.getControllerRestService()));
    }

    public String sendHttpGetToBroker(String table, String path)
    {
        return doHttpActionWithHeaders(
                Request.builder().prepareGet().setUri(URI.create(String.format("http://%s/%s", getBrokerHost(table), path))),
                Optional.empty(),
                Optional.empty());
    }

    private String getControllerUrl()
    {
        return pinotConfig.getControllerUrl();
    }

    @SuppressWarnings("unchecked")
    public List<String> getAllTables()
    {
        String responseBody = sendHttpGetToController(GET_ALL_TABLES_API_TEMPLATE);
        JSONObject jsonObject = JSONObject.parseObject(responseBody);
        JSONArray tables = jsonObject.getJSONArray("tables");
        if (tables == null) {
            throw new PinotException(PinotErrorCode.PINOT_DECODE_ERROR, Optional.empty(), "tables not found");
        }
        return Arrays.asList(tables.toArray(new String[tables.size()]));
    }

    public Schema getTableSchema(String table)
            throws Exception
    {
        String responseBody = sendHttpGetToController(String.format(TABLE_SCHEMA_API_TEMPLATE, table));
        return Schema.fromString(responseBody);
    }

    @VisibleForTesting
    List<String> getAllBrokersForTable(String table)
    {
        String responseBody = sendHttpGetToController(String.format(TABLE_INSTANCES_API_TEMPLATE, table));
        Set<String> allBrokers = new HashSet<>();
        try {
            JSONObject instancesResponse = JSONObject.parseObject(responseBody);
            JSONArray brokers = instancesResponse.getJSONArray("brokers");
            for (int i = 0; i < brokers.size(); ++i) {
                JSONObject broker = brokers.getJSONObject(i);
                JSONArray instances = broker.getJSONArray("instances");
                for (int j = 0; j < instances.size(); ++j) {
                    String brokerToParse = instances.getString(j);
                    Matcher matcher = BROKER_PATTERN.matcher(brokerToParse);
                    if (matcher.matches() && matcher.groupCount() == 2) {
                        allBrokers.add(matcher.group(1) + ":" + matcher.group(2));
                    }
                    else {
                        throw new PinotException(PinotErrorCode.PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), String.format("Cannot parse %s in the broker instance", brokerToParse));
                    }
                }
            }
            List<String> allBrokersList = new ArrayList<>(allBrokers);
            Collections.shuffle(allBrokersList);
            return ImmutableList.copyOf(allBrokersList);
        }
        catch (Exception e) {
            throw new PinotException(PinotErrorCode.PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), String.format("Cannot parse the brokers from the response %s for table %s", responseBody, table), e);
        }
    }

    public String getBrokerHost(String table)
    {
        try {
            List<String> brokers = brokersForTableCache.get(table);
            if (brokers.isEmpty()) {
                throw new PinotException(PinotErrorCode.PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), "No valid brokers found for " + table);
            }
            return brokers.get(ThreadLocalRandom.current().nextInt(brokers.size()));
        }
        catch (ExecutionException e) {
            Throwable err = e.getCause();
            if (err instanceof PinotException) {
                throw (PinotException) err;
            }
            else {
                throw new PinotException(PinotErrorCode.PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), "Error when getting brokers for table " + table, err);
            }
        }
    }

    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
            throws Exception
    {
        final Map<String, Map<String, List<String>>> routingTableMap = new HashMap<>();
        log.debug("Trying to get routingTable for %s from broker", tableName);
        String responseBody = sendHttpGetToBroker(tableName, String.format(ROUTING_TABLE_API_TEMPLATE, tableName));
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
    {
        String responseBody = sendHttpGetToBroker(table, String.format(TIME_BOUNDARY_API_TEMPLATE, table));
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
