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

import javafx.util.Pair;

import javax.inject.Inject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * This class is responsble for top level schema/deployment operations and has a unified view of all info regarding query table, such as
 * - Creating namespace->table->deployment hierarchy
 * - Preloading/making all RTAMS calls
 * - Caching
 */
public class RTASchemaHandler
{
    private RTAMSClient client;
    private Map<Pair<String, String>, RTATableEntity> tableMap;

    @Inject
    RTASchemaHandler(RTAMSClient client)
            throws IOException
    {
        this.client = client;
        tableMap = new HashMap<>();
        preload();
    }

    private void preload()
            throws IOException
    {
        List<String> namespaces = client.getNamespaces();
        for (String namespace : namespaces) {
            List<String> tables = client.getTables(namespace);
            for (String table : tables) {
                RTATableEntity entity = getTableEntity(namespace, table);
                tableMap.put(new Pair<>(namespace, table), entity);
            }
        }
    }

    private RTATableEntity getTableEntity(String namespace, String table)
            throws IOException
    {
        List<RTADeployment> deployments = client.getDeployments(namespace, table);
        RTADefinition definition = client.getDefinition(namespace, table);
        return new RTATableEntity(deployments, definition);
    }

    public RTADefinition getSchema(String namespace, String table)
    {
        return getEntity(namespace, table).definition;
    }

    public RTADeployment getAresDeployment(String namespace, String table)
    {
        for (RTADeployment deployment : getEntity(namespace, table).deployments) {
            if (deployment.getStorageType() == RTADeployment.StorageType.ARESDB) {
                // Return the first one
                return deployment;
            }
        }
        return null;
    }

    public List<RTADefinition.Field> getTimestampFields(String namespace, String table)
    {
        return getEntity(namespace, table).definition.getFields().stream().filter(t -> t.getColumnType().equals("time")).collect(Collectors.toList());
    }

    private RTATableEntity getEntity(String namespace, String table)
    {
        RTATableEntity entity = tableMap.get(new Pair<>(namespace, table));
        if (entity == null) {
            throw new NoSuchElementException(String.format("Can't find deployments for namespace %s and table %s", namespace, table));
        }
        return entity;
    }

    public RTADeployment getDefaultDeployment(String namespace, String table)
    {
        // Algorithm is simple for now, if there are pinot/aresdb deployments, we go pinot. If there are multiple aresdb deployments, we go with the first one.
        List<RTADeployment> deployments = getEntity(namespace, table).deployments;
        for (RTADeployment deployment : deployments) {
            if (deployment.getStorageType() == RTADeployment.StorageType.PINOT) {
                // Return the first one
                return deployment;
            }
        }
        if (deployments.size() > 0) {
            // Only aresdb deployments, return first one
            return deployments.get(0);
        }
        return null;
    }

    static class RTATableEntity
    {
        private List<RTADeployment> deployments;
        private RTADefinition definition;

        RTATableEntity(List<RTADeployment> deployments, RTADefinition definition)
        {
            this.deployments = deployments;
            this.definition = definition;
        }
    }
}
