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

package com.facebook.presto.aresdb;

import com.facebook.presto.aresdb.AresDbTable.AresDbColumn;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;

import javax.inject.Inject;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_HTTP_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_MULT_CHOICE;
import static java.net.HttpURLConnection.HTTP_OK;

public class AresDbConnection
{
    private static final String REQUEST_PAYLOAD_TEMPLATE = "{ \"queries\": [ %s ] }";

    private final AresDbConfig aresDbConfig;
    private final HttpClient httpClient;

    private final LoadingCache<String, AresDbTable> aresDbTableCache;
    private final Supplier<List<String>> allTablesCache;

    @Inject
    public AresDbConnection(AresDbConfig aresDbConfig, @ForAresDb HttpClient httpClient)
    {
        final long cacheExpiryMs = aresDbConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.aresDbConfig = aresDbConfig;
        this.httpClient = httpClient;

        this.allTablesCache = Suppliers.memoizeWithExpiration(
                () -> {
                    try {
                        return ImmutableList.of("rta_eats_order");
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                },
                cacheExpiryMs,
                TimeUnit.MILLISECONDS);

        this.aresDbTableCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build(new CacheLoader<String, AresDbTable>()
                        {
                            @Override
                            public AresDbTable load(String tableName)
                                    throws Exception
                            {
                                List<AresDbColumn> columns = new ArrayList<>();
                                columns.add(new AresDbColumn("workflowUUID", VARCHAR, false));
                                columns.add(new AresDbColumn("createdAt", BIGINT, true));
                                columns.add(new AresDbColumn("vehicleViewId", BIGINT, false));
                                columns.add(new AresDbColumn("tenancy", VARCHAR, false));
                                return new AresDbTable(tableName, columns);
                            }
                        });
    }

    private static boolean isValidAresDbHttpResponseCode(int status)
    {
        return status >= HTTP_OK && status < HTTP_MULT_CHOICE;
    }

    public AresDbTable getTable(String tableName)
    {
        try {
            return aresDbTableCache.get(tableName);
        }
        catch (Exception e) {
            // TODO: check for NOT_FOUND and throw better exceptions
            throw new AresDbException(ARESDB_HTTP_ERROR, "Failed to get table info from AresDb: " + tableName);
        }
    }

    public List<String> getTables()
    {
        try {
            return allTablesCache.get();
        }
        catch (Exception e) {
            // TODO: check for NOT_FOUND and throw better exceptions
            throw new AresDbException(ARESDB_HTTP_ERROR, "Failed to get list of tables from AresDB");
        }
    }

    public Optional<String> getTimeColumn(String tableName)
    {
        try {
            AresDbTable aresDbTable = aresDbTableCache.get(tableName);
            for (AresDbColumn column : aresDbTable.getColumns()) {
                if (column.isTimeColumn()) {
                    return Optional.of(column.getName());
                }
            }

            return Optional.empty();
        }
        catch (Exception e) {
            // TODO: check for NOT_FOUND and throw better exceptions
            throw new AresDbException(ARESDB_HTTP_ERROR, "Failed to get table info from AresDb: " + tableName);
        }
    }

    private String getTablesUrl()
    {
        return "http://" + aresDbConfig.getMetadataServiceUrl() + "/tables/definitions";
    }

    private String getTableDefinitionUrl(String tableName)
    {
        checkArgument(tableName != null && !tableName.isEmpty(), "Not a valid table name");
        return "http://" + aresDbConfig.getMetadataServiceUrl() + "/tables/definitions/" + tableName;
    }

    public String queryAndGetResults(String aql)
    {
        String payload = format(REQUEST_PAYLOAD_TEMPLATE, aql);
        Request.Builder requestBuilder = Request.builder()
                .prepareGet()
                .setUri(URI.create("http://" + aresDbConfig.getServiceUrl() + "/query/aql"));

        requestBuilder = requestBuilder.setBodyGenerator(createStaticBodyGenerator(payload, StandardCharsets.UTF_8));

        StringResponse stringResponse = requestWithProxyServiceHeaders(requestBuilder, aresDbConfig.getServiceName());

        if (isValidAresDbHttpResponseCode(stringResponse.getStatusCode())) {
            return stringResponse.getBody();
        }
        else {
            throw new AresDbException(ARESDB_HTTP_ERROR,
                    format("Unexpected response status from AresDB: status code: %s, error: %s", stringResponse.getStatusCode(), stringResponse.getBody()),
                    payload);
        }
    }

    private StringResponse requestWithProxyServiceHeaders(Request.Builder requestBuilder, String serviceName)
    {
        Request request = requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setHeader(aresDbConfig.getCallerHeaderParam(), aresDbConfig.getCallerHeaderValue())
                .setHeader(aresDbConfig.getServiceHeaderParam(), serviceName)
                .build();

        return httpClient.execute(request, createStringResponseHandler());
    }
}
