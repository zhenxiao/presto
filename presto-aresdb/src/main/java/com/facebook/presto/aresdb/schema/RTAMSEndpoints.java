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

public final class RTAMSEndpoints
{
    private static final String RTAMS_HOST = "localhost";
    private static final int RTAMS_PORT = 5436;
    private static final String NAMESPACE_ENDPOINT = "namespaces";
    // Parameter: namespace
    private static final String TABLES_UNDER_NAMESPACE = "namespaces/%s";
    // Parameters: namespace, table
    private static final String SCHEMA_FROM_NAMESPACE_TABLE = "tables/definitions/%s/%s";
    private static final String DEPLOYMENT_FROM_NAMESPACE_TABLE = "tables/%s/%s/deployments";

    private RTAMSEndpoints()
    {
    }

    public static String getURL()
    {
        return String.format("http://%s:%d", RTAMS_HOST, RTAMS_PORT);
    }

    public static String getNamespaces()
    {
        return String.format("%s/%s", getURL(), NAMESPACE_ENDPOINT);
    }

    public static String getTablesFromNamespace(String namespace)
    {
        return String.format("%s/%s", getNamespaces(), namespace);
    }

    public static String getTableSchema(String namespace, String tableName)
    {
        String tableSchemaEndpoint = String.format(SCHEMA_FROM_NAMESPACE_TABLE, namespace, tableName);
        return String.format("%s/%s", getURL(), tableSchemaEndpoint);
    }

    public static String getDeployment(String namespace, String tableName)
    {
        String tableDeploymentEndpoint = String.format(DEPLOYMENT_FROM_NAMESPACE_TABLE, namespace, tableName);
        return String.format("%s/%s", getURL(), tableDeploymentEndpoint);
    }
}
