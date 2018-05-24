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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.NestedField;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(ElasticsearchSplitManager.class);

    private final String connectorId;
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchConnectorId connectorId, ElasticsearchClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy, Optional<Map<String, NestedField>> nestedFields, Optional<Map<String, String>> jsonPaths, Optional<Long> limit)
    {
        ElasticsearchTableLayoutHandle layoutHandle = (ElasticsearchTableLayoutHandle) layout;
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();
        ElasticsearchTableDescription table = client.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<String> indices = client.getIndices(table);
        for (String index : indices) {
            log.info("index: " + index);
        }
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (String index : indices) {
            ClusterSearchShardsResponse response = client.getSearchShards(index, table);
            DiscoveryNode[] nodes = response.getNodes();
            for (ClusterSearchShardsGroup group : response.getGroups()) {
                int nodeIndex = group.getShardId() % nodes.length;
                ElasticsearchSplit split = new ElasticsearchSplit(connectorId, table, index, group.getShardId(), nodes[nodeIndex].getName(), nodes[nodeIndex].getAddress().getPort(), layoutHandle.getTupleDomain(), jsonPaths, limit, nestedFields, layoutHandle.getNestedTupleDomain());
                log.info("split: " + " index: " + index + " shardId: " + group.getShardId() + " node: " + nodes[nodeIndex].getName().toString() + " port: " + nodes[nodeIndex].getAddress().getPort());
                splits.add(split);
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
