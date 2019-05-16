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

import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class TestRTAClient
{
    @Test
    public void testDeployments()
            throws IOException
    {
        List<RTADeployment> deployments = TestSchemaUtils.getDeployments();
        Assert.assertEquals(deployments.size(), 1);
        RTADeployment deployment = deployments.get(0);
        Assert.assertEquals(deployment.getCluster(), "stagingb");
        Assert.assertEquals(deployment.getName(), "rta_eats_order");
        Assert.assertEquals(deployment.getNamespace(), "rta");
        Assert.assertEquals(deployment.getStorageType(), RTADeployment.StorageType.ARESDB);
        Assert.assertEquals(deployment.getPhysicalSchema().getColumns().size(), 4);
    }

    @Test
    public void testDefinition()
            throws IOException
    {
        RTADefinition definiton = TestSchemaUtils.getDefinition();
        Assert.assertEquals(definiton.getFields().size(), 4);
        Assert.assertEquals(definiton.getMetadata().getPrimaryKeys().toArray(), new String[]{"workflowUUID"});
        Assert.assertTrue(definiton.getMetadata().isFactTable());
        Assert.assertEquals(definiton.getMetadata().getQueryTypes().toArray(), new String[]{"pre_defined"});
        Assert.assertEquals(definiton.getFields().get(0).getCardinality(), "high");
        Assert.assertEquals(definiton.getFields().get(0).getColumnType(), "metric");
        Assert.assertEquals(definiton.getFields().get(0).getName(), "workflowUUID");
        Assert.assertEquals(definiton.getFields().get(0).getUberLogicalType(), "String");
        Assert.assertEquals(definiton.getFields().get(0).getType(), "string");
    }

    @Test
    public void testNamespaces()
            throws IOException
    {
        List<String> namespaces = TestSchemaUtils.getNamespaces();
        Assert.assertEquals(namespaces.toArray(), new String[]{"rta"});
    }

    @Test
    public void testTables()
            throws IOException
    {
        List<String> tables = TestSchemaUtils.getTables();
        Assert.assertEquals(tables.size(), 14);
    }
}
