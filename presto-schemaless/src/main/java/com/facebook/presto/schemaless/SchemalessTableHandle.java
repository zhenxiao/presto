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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Schemaless specific {@link ConnectorTableHandle}.
 */
public class SchemalessTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String dataStoreName;
    private final String indexTableName;

    @JsonCreator
    public SchemalessTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("dataStoreName") String schemaName,
            @JsonProperty("indexTableName") String tableName)
    {
        this.connectorId = requireNonNull(connectorId.toLowerCase(ENGLISH), "connectorId is null");
        this.dataStoreName = requireNonNull(schemaName.toLowerCase(ENGLISH), "dataStoreName is null");
        this.indexTableName = requireNonNull(tableName.toLowerCase(ENGLISH), "indexTableName is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getDataStoreName()
    {
        return dataStoreName;
    }

    @JsonProperty
    public String getIndexTableName()
    {
        return indexTableName;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(dataStoreName, indexTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, dataStoreName, indexTableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        SchemalessTableHandle other = (SchemalessTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.dataStoreName, other.dataStoreName) &&
                Objects.equals(this.indexTableName, other.indexTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, dataStoreName, indexTableName);
    }
}
