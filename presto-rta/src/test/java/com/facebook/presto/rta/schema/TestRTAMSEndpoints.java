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

package com.facebook.presto.rta.schema;

import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.Test;

public class TestRTAMSEndpoints
{
    @Test
    public void testNamespaceEndpoint()
    {
        Assert.assertEquals(RTAMSEndpoints.getNamespaces(), "http://localhost:5436/namespaces");
    }

    @Test
    public void testTablesEndpoint()
    {
        Assert.assertEquals(RTAMSEndpoints.getTablesFromNamespace("rta"), "http://localhost:5436/namespaces/rta");
    }

    @Test
    public void testSchemaEndpoint()
    {
        Assert.assertEquals(RTAMSEndpoints.getTableSchema("rta", "table"), "http://localhost:5436/tables/definitions/rta/table");
    }

    @Test
    public void testDeploymentEndpoint()
    {
        Assert.assertEquals(RTAMSEndpoints.getDeployment("rta", "table"), "http://localhost:5436/tables/rta/table/deployments");
    }
}
