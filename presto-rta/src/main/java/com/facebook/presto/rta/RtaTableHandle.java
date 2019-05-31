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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RtaTableHandle
        implements ConnectorTableHandle
{
    private final RtaConnectorId connectorId;
    private final RtaStorageKey key;
    private final SchemaTableName schemaTableName;
    private final ConnectorTableHandle handle;

    @JsonCreator
    public RtaTableHandle(@JsonProperty("connectorId") RtaConnectorId connectorId, @JsonProperty("key") RtaStorageKey key, @JsonProperty("schemaTableName") SchemaTableName schemaTableName, @JsonProperty("handle") ConnectorTableHandle handle)
    {
        this.connectorId = connectorId;
        this.key = key;
        this.schemaTableName = schemaTableName;
        this.handle = handle;
    }

    @JsonProperty
    public RtaConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorTableHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public RtaStorageKey getKey()
    {
        return key;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RtaTableHandle that = (RtaTableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(key, that.key) &&
                Objects.equals(handle, that.handle) &&
                Objects.equals(schemaTableName, that.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, key, handle, schemaTableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("key", key)
                .add("handle", handle)
                .add("schemaTableName", schemaTableName)
                .toString();
    }
}
