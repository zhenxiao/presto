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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Json description to parse a Schemaless table.
 */
public class SchemalessTableDescription
{
    private final String indexTableName;
    private final String dataStoreName;
    private final String hostAddress;
    private final int port;
    private final String instanceName;
    private SchemalessIndexDefinition indexDefinition;

    @JsonCreator
    public SchemalessTableDescription(
            @JsonProperty("indexTableName") String indexTableName,
            @JsonProperty("dataStoreName") String dataStoreName,
            @JsonProperty("hostAddress") String hostAddress,
            @JsonProperty("port") int port,
            @JsonProperty("instanceName") String instanceName)
    {
        checkArgument(!isNullOrEmpty(indexTableName), "indexTableName is null or is empty");
        checkArgument(!isNullOrEmpty(hostAddress), "hostAddress is null or is empty");
        checkArgument(!isNullOrEmpty(instanceName), "instanceName is null or is empty");
        checkArgument(!isNullOrEmpty(dataStoreName), "dataStoreName is null or is empty");
        this.indexTableName = indexTableName;
        this.dataStoreName = dataStoreName;
        this.hostAddress = hostAddress;
        this.port = port;
        this.instanceName = instanceName;
    }

    @JsonProperty
    public String getIndexTableName()
    {
        return indexTableName;
    }

    @JsonProperty
    public String getDataStoreName()
    {
        return dataStoreName;
    }

    @JsonProperty
    public String getHostAddress()
    {
        return hostAddress;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @JsonProperty
    public String getInstanceName()
    {
        return instanceName;
    }

    @JsonProperty
    public SchemalessIndexDefinition getIndexDefinition()
    {
        return indexDefinition;
    }

    public void setIndexDefinition(SchemalessIndexDefinition indexDefinition)
    {
        this.indexDefinition = indexDefinition;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexTableName", indexTableName)
                .add("dataStoreName", dataStoreName)
                .add("hostAddress", hostAddress)
                .add("port", port)
                .add("instanceName", instanceName)
                .add("indexDefinition", indexDefinition)
                .toString();
    }
}
