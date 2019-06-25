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
package com.facebook.presto.aresdb;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class AresDbTableHandle
        implements ConnectorTableHandle
{
    private final AresDbConnectorId connectorId;
    private final String tableName;
    private final Optional<String> timeColumnName;
    private final Optional<Type> timeColumnType;
    private final Optional<Duration> retention;

    @JsonCreator
    public AresDbTableHandle(
            @JsonProperty("connectorId") AresDbConnectorId connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeColumnName") Optional<String> timeColumnName,
            @JsonProperty("timeColumnType") Optional<Type> timeColumnType,
            @JsonProperty("retention") Optional<Duration> retention)
    {
        this.connectorId = connectorId;
        this.tableName = tableName;
        this.timeColumnName = timeColumnName;
        this.timeColumnType = timeColumnType;
        this.retention = retention;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("tableName", tableName)
                .add("timeColumnName", timeColumnName)
                .add("timeColumnType", timeColumnType)
                .add("retention", retention)
                .toString();
    }

    @JsonProperty
    public AresDbConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<String> getTimeColumnName()
    {
        return timeColumnName;
    }

    @JsonProperty
    public Optional<Duration> getRetention()
    {
        return retention;
    }

    @JsonProperty
    public Optional<Type> getTimeColumnType()
    {
        return timeColumnType;
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

        AresDbTableHandle that = (AresDbTableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(timeColumnName, that.timeColumnName) &&
                Objects.equals(timeColumnType, that.timeColumnType) &&
                Objects.equals(retention, that.retention);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, tableName, timeColumnName, timeColumnType, retention);
    }
}
