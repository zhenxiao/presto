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
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class TestRTASchemaHandler
{
    private RTASchemaHandler handler;

    @BeforeClass
    public void beforeClass()
            throws IOException
    {
        RTAMSClient client = Mockito.mock(RTAMSClient.class);
        Mockito.when(client.getNamespaces()).thenReturn(Arrays.asList("rta"));
        Mockito.when(client.getTables("rta")).thenReturn(Arrays.asList("rta_eats_order"));
        Mockito.doReturn(TestSchemaUtils.getDefinition()).when(client).getDefinition("rta", "rta_eats_order");
        Mockito.doReturn(TestSchemaUtils.getDeployments()).when(client).getDeployments("rta", "rta_eats_order");
        Mockito.doReturn(TestSchemaUtils.getDeployments()).when(client).getDeployments("rta", "rta_eats_order");
        handler = new RTASchemaHandler(client);
    }

    @Test
    public void testGetTimestampFields()
    {
        List<RTADefinition.Field> timestampFields = handler.getTimestampFields("rta", "rta_eats_order");
        Assert.assertEquals(timestampFields.toArray(), new RTADefinition.Field[]{new RTADefinition.Field("long", "createdAt", "Long", "time", "high")});
    }

    @Test
    public void testGetAresDeployment()
    {
        RTADeployment deployment = handler.getAresDeployment("rta", "rta_eats_order");
        Assert.assertNotNull(deployment);
        Assert.assertEquals(deployment.getCluster(), "stagingb");
    }

    @Test
    public void testGetDefaultDeployment()
    {
        RTADeployment deployment = handler.getDefaultDeployment("rta", "rta_eats_order");
        Assert.assertNotNull(deployment);
        Assert.assertEquals(deployment.getCluster(), "stagingb");
    }

    @Test
    public void testBadDeploymentRequest()
    {
        try {
            RTADeployment deployment = handler.getDefaultDeployment("rta", "not_exists");
            Assert.fail("Should have failed here");
        }
        catch (NoSuchElementException e) {
            Assert.assertTrue(e.getMessage().startsWith("Can't find deployments for namespace"));
        }
    }
}
