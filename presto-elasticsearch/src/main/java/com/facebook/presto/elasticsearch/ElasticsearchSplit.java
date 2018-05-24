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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.NestedField;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final ElasticsearchTableDescription table;
    private final String index;
    private final int shard;
    private final String searchNode;
    private final int port;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<Map<String, String>> jsonPaths;
    private final Optional<Long> limit;
    private final Optional<Map<String, NestedField>> nestedFields;
    private final Optional<TupleDomain<List<String>>> nestedTupleDomain;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("table") ElasticsearchTableDescription table,
            @JsonProperty("index") String index,
            @JsonProperty("shard") int shard,
            @JsonProperty("searchNode") String searchNode,
            @JsonProperty("port") int port,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("jsonPaths") Optional<Map<String, String>> jsonPaths,
            @JsonProperty("limit") Optional<Long> limit,
            @JsonProperty("nestedFields") Optional<Map<String, NestedField>> nestedFields,
            @JsonProperty("nestedTupleDomain") Optional<TupleDomain<List<String>>> nestedTupleDomain)
    {
        this.shard = shard;
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.table = requireNonNull(table, "table is null");
        this.index = requireNonNull(index, "index is null");
        this.searchNode = requireNonNull(searchNode, "searchNode is null");
        this.port = requireNonNull(port, "port is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.jsonPaths = requireNonNull(jsonPaths, "jsonPaths is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.nestedFields = requireNonNull(nestedFields, "nestedFields is null");
        this.nestedTupleDomain = requireNonNull(nestedTupleDomain, "nestedTupleDomain is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ElasticsearchTableDescription getTable()
    {
        return table;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }

    @JsonProperty
    public String getSearchNode()
    {
        return searchNode;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public Optional<Map<String, String>> getJsonPaths()
    {
        return jsonPaths;
    }

    @JsonProperty
    public Optional<Long> getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<Map<String, NestedField>> getNestedFields()
    {
        return nestedFields;
    }

    @JsonProperty
    public Optional<TupleDomain<List<String>>> getNestedTupleDomain()
    {
        return nestedTupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(HostAddress.fromParts(searchNode, port));
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(connectorId)
                .addValue(table.getSchemaName())
                .addValue(table.getTableName())
                .addValue(table.getHostAddress())
                .addValue(table.getClusterName())
                .addValue(table.getIndexPrefix())
                .addValue(index)
                .addValue(shard)
                .addValue(port)
                .addValue(searchNode)
                .addValue(tupleDomain)
                .addValue(jsonPaths)
                .addValue(limit)
                .addValue(nestedFields)
                .addValue(nestedTupleDomain)
                .toString();
    }
}
