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
package com.facebook.presto.elasticsearch2;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.facebook.presto.elasticsearch2.ElasticsearchErrorCode.ELASTIC_SEARCH_MAPPING_REQUEST_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient
{
    private static final Logger log = Logger.get(ElasticsearchClient.class);

    private final Map<SchemaTableName, ElasticsearchTableDescription> tableDescriptions;
    private final Map<String, Client> clients = new HashMap<String, Client>();
    private final Duration timeout;

    @Inject
    public ElasticsearchClient(Map<SchemaTableName, ElasticsearchTableDescription> descriptions, Duration requestTimeout)
            throws IOException
    {
        tableDescriptions = requireNonNull(descriptions, "Elasticsearch Table Description is null");
        timeout = requireNonNull(requestTimeout, "Elasticsearch request timeout is null");

        for (Map.Entry<SchemaTableName, ElasticsearchTableDescription> entry : tableDescriptions.entrySet()) {
            ElasticsearchTableDescription tableDescription = entry.getValue();
            if (!clients.containsKey(tableDescription.getClusterName())) {
                Settings settings = Settings.settingsBuilder().put("cluster.name", tableDescription.getClusterName()).build();
                TransportClient client = TransportClient.builder().settings(settings).build();
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(tableDescription.getHostAddress()), tableDescription.getPort()));
                clients.put(tableDescription.getClusterName(), client);
            }
        }
    }

    public List<String> listSchemas()
    {
        return tableDescriptions.keySet().stream().map(SchemaTableName::getSchemaName).collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        return tableDescriptions.keySet().stream().filter(schemaTableName -> schemaNameOrNull == null || schemaTableName.getSchemaName().equals(schemaNameOrNull)).collect(toImmutableList());
    }

    public ElasticsearchTableDescription getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        for (Map.Entry<SchemaTableName, ElasticsearchTableDescription> entry : tableDescriptions.entrySet()) {
            buildColumns(entry.getValue());
        }
        return tableDescriptions.get(new SchemaTableName(schemaName, tableName));
    }

    public List<String> getIndices(ElasticsearchTableDescription tableDescription)
    {
        if (tableDescription.getIndexExactMatch()) {
            return ImmutableList.of(tableDescription.getIndexPrefix());
        }
        Client client = clients.get(tableDescription.getClusterName());
        verify(client != null);
        return Arrays.stream(client.admin().indices().getIndex(new GetIndexRequest()).actionGet(timeout.toMillis()).getIndices())
                    .filter(index -> index.startsWith(tableDescription.getIndexPrefix()))
                    .collect(toImmutableList());
    }

    public ClusterSearchShardsResponse getSearchShards(String index, ElasticsearchTableDescription tableDescription)
    {
        Client client = clients.get(tableDescription.getClusterName());
        verify(client != null);
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest(index);
        return client.admin().cluster().searchShards(request).actionGet(timeout.toMillis());
    }

    private void buildColumns(ElasticsearchTableDescription tableDescription)
    {
        Set<ElasticsearchColumn> columns = new HashSet();
        Client client = clients.get(tableDescription.getClusterName());
        verify(client != null);
        for (String index : getIndices(tableDescription)) {
            try {
                GetMappingsRequest mappingsRequest = new GetMappingsRequest().types(tableDescription.getType());

                if (!isNullOrEmpty(index)) {
                    mappingsRequest.indices(index);
                }
                ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client.admin().indices().getMappings(mappingsRequest).actionGet(timeout.toMillis()).getMappings();

                Iterator<String> indexIterator = mappings.keysIt();
                while (indexIterator.hasNext()) {
                    MappingMetaData mappingMetaData = mappings.get(indexIterator.next()).get(tableDescription.getType());
                    JSONObject mappingObject = new JSONObject(mappingMetaData.source().toString()).getJSONObject(tableDescription.getType());
                    JSONObject properties = mappingObject.getJSONObject("properties");
                    List<String> lists = new ArrayList<String>();
                    if (properties.has("_meta")) {
                        JSONObject meta = mappingObject.getJSONObject("_meta");
                        if (meta.has("lists")) {
                            JSONArray arrays = meta.getJSONArray("lists");
                            for (int i = 0; i < arrays.length(); i++) {
                                lists.add(arrays.getString(i));
                            }
                        }
                    }
                    populateColumns(properties, lists, columns);
                }
                tableDescription.setColumns(ImmutableList.copyOf(columns));
                tableDescription.setColumnsMetadata(columns.stream().map(ElasticsearchColumnMetadata::new).collect(Collectors.toList()));
            }
            catch (JSONException e) {
                throw new PrestoException(ELASTIC_SEARCH_MAPPING_REQUEST_ERROR, e);
            }
        }
    }

    private List<String> getColumnsMetadata(String parent, JSONObject json)
            throws JSONException
    {
        List<String> leaves = new ArrayList();
        Iterator it = json.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            Object child = json.get(key);
            String childKey = parent == null || parent.isEmpty() ? key : parent.concat(".").concat(key);

            if (child instanceof JSONObject) {
                leaves.addAll(getColumnsMetadata(childKey, (JSONObject) child));
            }
            else if (child instanceof JSONArray) {
                continue;
            }
            else {
                leaves.add(childKey.concat(":").concat(child.toString()));
            }
        }
        return leaves;
    }

    private void populateColumns(JSONObject properties, List<String> arrays, Set<ElasticsearchColumn> columns)
            throws JSONException
    {
        TreeMap<String, Type> fieldsMap = new TreeMap<>(new Comparator<String>()
        {
            @Override
            public int compare(String key1, String key2)
            {
                int length1 = key1.split("\\.").length;
                int length2 = key2.split("\\.").length;
                if (length1 == length2) {
                    return key1.compareTo(key2);
                }
                return length2 - length1;
            }
        });
        for (String columnMetadata : getColumnsMetadata(null, properties)) {
            String[] items = columnMetadata.split(":");
            if (items.length != 2) {
                log.debug("Invalid column path format: " + columnMetadata);
                continue;
            }

            if (!items[0].endsWith(".type")) {
                log.debug("Ignore column with no type info: " + columnMetadata);
                continue;
            }
            String propertyName = items[0].substring(0, items[0].lastIndexOf('.'));
            String nestedName = propertyName.replaceAll("properties\\.", "");
            if (nestedName.contains(".")) {
                fieldsMap.put(nestedName, getPrestoType(items[1]));
            }
            else {
                if (!columns.stream().anyMatch(column -> column.getName().equalsIgnoreCase(nestedName))) {
                    columns.add(new ElasticsearchColumn(nestedName, getPrestoType(items[1]), nestedName, items[1], arrays.contains(nestedName)));
                }
            }
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private void processNestedFields(TreeMap<String, Type> fieldsMap, Set<ElasticsearchColumn> columns, List<String> arrays)
    {
        if (fieldsMap.size() == 0) {
            return;
        }
        Map.Entry<String, Type> first = fieldsMap.firstEntry();
        String field = first.getKey();
        Type type = first.getValue();
        if (field.contains(".")) {
            String prefix = field.substring(0, field.lastIndexOf('.'));
            ImmutableList.Builder<RowType.Field> fieldsBuilder = ImmutableList.builder();
            int size = field.split("\\.").length;
            Iterator<String> iterator = fieldsMap.navigableKeySet().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next();
                if (name.split("\\.").length == size && name.startsWith(prefix)) {
                    fieldsBuilder.add(new RowType.Field(Optional.of(name.substring(name.lastIndexOf('.') + 1)), fieldsMap.get(name)));
                    iterator.remove();
                    continue;
                }
                break;
            }
            fieldsMap.put(prefix, RowType.from(fieldsBuilder.build()));
        }
        else {
            if (!columns.stream().anyMatch(column -> column.getName().equalsIgnoreCase(field))) {
                columns.add(new ElasticsearchColumn(field, type, field, type.getDisplayName(), arrays.contains(field)));
            }
            fieldsMap.remove(field);
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private static Type getPrestoType(String elasticsearchType)
    {
        switch (elasticsearchType) {
            case "double":
            case "float":
                return DOUBLE;
            case "integer":
                return INTEGER;
            case "long":
                return BIGINT;
            case "string":
                return VARCHAR;
            case "boolean":
                return BOOLEAN;
            case "binary":
                return VARBINARY;
            default:
                return VARCHAR;
        }
    }
}
