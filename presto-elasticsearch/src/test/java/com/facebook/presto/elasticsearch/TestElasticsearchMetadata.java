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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.elasticsearch.ElasticsearchConnectorModule.createElasticsearchClient;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestElasticsearchMetadata
{
    private static final JsonCodec<ElasticsearchTableDescription> TABLE_CODEC;
    private ElasticsearchMetadata metadata;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        TABLE_CODEC = codecFactory.jsonCodec(ElasticsearchTableDescription.class);
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        ElasticsearchConnectorConfig config = new ElasticsearchConnectorConfig();
        URL metadataUrl = Resources.getResource(TestElasticsearchMetadata.class, "/elasticsearch");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();
        config.setTableDescriptionDir(new File(metadataUri));
        config.setTableNames("example.table1,example.table2");
        config.setDefaultSchema("example");
        config.setScrollSize(1000);
        config.setScrollTime(new Duration(60, TimeUnit.SECONDS));

        ElasticsearchClient client = createElasticsearchClient(config, TABLE_CODEC);
        metadata = new ElasticsearchMetadata(new ElasticsearchConnectorId("test"), client);
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
}
