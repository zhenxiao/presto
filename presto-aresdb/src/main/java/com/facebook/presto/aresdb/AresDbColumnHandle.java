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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class AresDbColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type dataType;
    private final AresDbColumnType type;

    @JsonCreator
    public AresDbColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("dataType") Type dataType,
            @JsonProperty("type") AresDbColumnType type)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getDataType()
    {
        return dataType;
    }

    @JsonProperty
    public AresDbColumnType getType()
    {
        return type;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, dataType);
    }

    public String getName()
    {
        return columnName;
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

        AresDbColumnHandle that = (AresDbColumnHandle) o;
        return Objects.equals(columnName, that.columnName) && Objects.equals(dataType, that.dataType) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("dataType", dataType)
                .add("type", type)
                .toString();
    }

    public enum AresDbColumnType
    {
        REGULAR, // refers to the column in table
        DERIVED, // refers to a derived column that is created after a pushdown expression
    }
}
