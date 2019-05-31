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
package com.facebook.presto.rta;

import com.facebook.presto.rta.schema.RTADeployment;
import com.facebook.presto.rta.schema.TestSchemaUtils;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestRtaPropertyManager
{
    private RtaConfig rtaConfig;

    @BeforeTest
    public void setUp()
    {
        rtaConfig = new RtaConfig().setConfigFile("");
    }

    @Test
    public void testDatacenterSorting()
            throws IOException
    {
        RtaPropertyManager rtaPropertyManager = new RtaPropertyManager(rtaConfig.setDataCenterOverride("phx8"));
        RTADeployment picked = rtaPropertyManager.pickDeployment(ImmutableList.of(
                new RTADeployment("aresdb", "nocare", "nocare", "nocare", "dca1", "{}"),
                new RTADeployment("pinot", "nocare", "nocare", "nocare", "phx7", "{}"),
                new RTADeployment("aresdb", "nocare", "nocare", "nocare", "phx7", "{}"),
                new RTADeployment("aresdb", "nocare", "nocare", "nocare", "phx8", "{}"),
                new RTADeployment("pinot", "nocare", "nocare", "nocare", "pHX8", "{}"),
                new RTADeployment("pinot", "somecare", "somecare", "nocare", "Phx8", "{}")));
        Assert.assertEquals(picked.getStorageType(), RtaStorageType.PINOT);
        Assert.assertTrue(picked.getDataCenter().equalsIgnoreCase("phx8"));
    }

    @Test
    public void testGetDefaultDeployment()
            throws IOException
    {
        RTADeployment deployment = new RtaPropertyManager(rtaConfig).pickDeployment(TestSchemaUtils.getDeployments());
        Assert.assertNotNull(deployment);
        Assert.assertEquals(deployment.getCluster(), "stagingb");
    }
}
