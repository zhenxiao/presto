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
package com.facebook.presto.schemaless;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.uber.schemaless.IndexDefinition;
import com.uber.schemaless.IndexField;
import com.uber.schemaless.SchemalessClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestSchemalessMetadata
{
    private static final JsonCodec<SchemalessTableDescription> TABLE_CODEC;
    private SchemalessMetadata metadata;
    private static final Map<String, IndexDefinition> definitionMap;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        TABLE_CODEC = codecFactory.jsonCodec(SchemalessTableDescription.class);
        definitionMap = new HashMap<>();
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        definitionMap.clear();
        definitionMap.put("example.table1", getIndexDefinition("1"));
        definitionMap.put("example.table2", getIndexDefinition("2"));
        SchemalessClient client = mock(SchemalessClient.class);

        SchemalessConnectorConfig config = new SchemalessConnectorConfig();
        URL metadataUrl = Resources.getResource(TestSchemalessMetadata.class, "/schemaless");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();
        config.setTableDescriptionDir(new File(metadataUri));
        config.setDefaultdataStore("example");
        config.setTableNames("example.table1,example.table2");
        SchemalessTableDescriptionSupplier tableDescriptionSupplier = SchemalessConnectorModule.createTableDescriptionSupplier(config, TABLE_CODEC);
        SchemalessClientManager clientManager = spy(SchemalessConnectorModule.createSchemalessClientManager(config, tableDescriptionSupplier));
        when(clientManager.getClient(anyString())).thenReturn(client);
        when(client.getIndexDefinitions(anyInt(), anyString(), eq(null))).thenReturn(definitionMap);
        metadata = new SchemalessMetadata(new SchemalessConnectorId("test"), clientManager);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("example"));
    }

    @Test
    public void testListTables()
    {
        SchemaTableName tableName1 = new SchemaTableName("example", "table1");
        SchemaTableName tableName2 = new SchemaTableName("example", "table2");
        List<SchemaTableName> tables = metadata.listTables(SESSION, "example");
        assertEquals(tables.size(), 2);
        assertEquals(tables.get(0), tableName1);
        assertEquals(tables.get(1), tableName2);
    }

    @Test
    public void testListTableColumns()
    {
        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("example", "table1"));
        assertEquals(columns.size(), 1);
        SchemaTableName table1 = new SchemaTableName("example", "table1");
        assertEquals(columns.get(table1), getTableColumns(table1, getIndexDefinition("1")));

        columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("example", "table2"));
        SchemaTableName table2 = new SchemaTableName("example", "table2");
        assertEquals(columns.get(table2), getTableColumns(table2, getIndexDefinition("2")));
    }

    private IndexDefinition getIndexDefinition(String suffix)
    {
        List<String> primaryKey = new ArrayList<>();
        primaryKey.add("key" + suffix);
        List<String> columnKeys = new ArrayList<>();
        columnKeys.add("key" + suffix);
        columnKeys.add("key" + suffix + "A");
        List<List<String>> compositeIndexes = new ArrayList<>();
        IndexDefinition indexDefn = new IndexDefinition();
        indexDefn.alias = "table" + suffix;
        indexDefn.kind = "kind" + suffix;
        indexDefn.state = "state" + suffix;
        indexDefn.columnKey = "key" + suffix;
        indexDefn.table = "table" + suffix;
        indexDefn.shardField = "key" + suffix;
        indexDefn.shardFieldExpr = "shardFieldExpr" + suffix;
        indexDefn.mutableShardKeyEnabled = false;
        indexDefn.primaryKey = primaryKey;
        indexDefn.useAddedId = false;
        indexDefn.formatVersion = 999L;
        indexDefn.columnKeys = columnKeys;
        indexDefn.fields = getIndexFields(suffix);
        indexDefn.compositeIndexes = compositeIndexes;
        return indexDefn;
    }

    private List<IndexField> getIndexFields(String suffix)
    {
        List<String> columnKeys1 = new ArrayList<>();
        columnKeys1.add("key" + suffix);
        IndexField field1 = new IndexField();
        field1.name = "key" + suffix;
        field1.columnKey = "key" + suffix;
        field1.sqlType = "varchar";
        field1.type = "string";
        field1.expr = "expr" + suffix;
        field1.columnKeys = columnKeys1;

        List<String> columnKeys2 = new ArrayList<>();
        columnKeys2.add("key" + suffix + "A");
        IndexField field2 = new IndexField();
        field2.name = "key" + suffix + "A";
        field2.columnKey = "key" + suffix + "A";
        field2.sqlType = "bool";
        field2.type = "bool";
        field2.expr = "expr" + suffix + "A";
        field2.columnKeys = columnKeys2;

        List<String> columnKeys3 = new ArrayList<>();
        columnKeys3.add("key" + suffix + "B");
        IndexField field3 = new IndexField();
        field3.name = "key" + suffix + "B";
        field3.columnKey = "key" + suffix + "B";
        field3.sqlType = "biginteger";
        field3.type = "biginteger";
        field3.expr = "expr" + suffix + "B";
        field3.columnKeys = columnKeys3;

        List<IndexField> results = new ArrayList<>();
        results.add(field1);
        results.add(field2);
        results.add(field3);
        return results;
    }

    private List<ColumnMetadata> getTableColumns(SchemaTableName tableName, IndexDefinition indexDefinition)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        SchemalessIndexDefinition schemalessIndexDefn = SchemalessClientManager.buildSchemalessIndexDefinition(indexDefinition);

        for (SchemalessIndexField field : schemalessIndexDefn.getFields()) {
            builder.add(field.getColumnMetadata());
        }

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, builder.build());
        return tableMetadata.getColumns();
    }
}
