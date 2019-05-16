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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * This class communicates with the rta-ums (muttley)/rtaums (udeploy) service. It's api is available here:
 * https://rtaums.uberinternal.com/tables/{namespace}/{tablename}/deployments
 */
public class RTADeployment
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private final PhysicalSchema physicalSchema;
    private final StorageType storageTypeEnum;

    @JsonProperty("storageType")
    private String storageType;

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("name")
    private String name;

    @JsonProperty("cluster")
    private String cluster;

    @JsonCreator
    public RTADeployment(@JsonProperty("storageType") String storageType, @JsonProperty("namespace") String namespace,
            @JsonProperty("name") String name, @JsonProperty("cluster") String cluster, @JsonProperty("schema") String schema)
            throws IOException
    {
        this.storageTypeEnum = StorageType.valueOf(storageType);
        this.namespace = namespace;
        this.name = name;
        this.cluster = cluster;
        this.physicalSchema = mapper.readValue(schema, PhysicalSchema.class);
    }

    public StorageType getStorageType()
    {
        return storageTypeEnum;
    }

    @JsonProperty
    public String getNamespace()
    {
        return namespace;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getCluster()
    {
        return cluster;
    }

    @JsonProperty
    public PhysicalSchema getPhysicalSchema()
    {
        return physicalSchema;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PhysicalSchema
    {
        @JsonProperty
        private List<Column> columns;

        public List<Column> getColumns()
        {
            return columns;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Column
        {
            @JsonProperty
            private String name;
            @JsonProperty
            private String type;
        }
    }

    public enum StorageType {
        ARESDB,
        PINOT
    }
}
