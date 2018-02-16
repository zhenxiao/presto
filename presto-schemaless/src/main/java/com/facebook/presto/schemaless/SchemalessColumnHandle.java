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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Schemaless specific connector column handle. Describes each column in a Schemaless table.
 * TODO Remove requirenonull constraints for optional fields later
 */
public class SchemalessColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final int ordinalPosition;
    private final String name;
    private final String columnKey;
    private final String sqlType;
    private final String fieldType;
    private final List<String> columnKeys;
    private final String expr;
    private final Type prestoType;

    @JsonCreator
    public SchemalessColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("name") String name,
            @JsonProperty("column_key") String columnKey,
            @JsonProperty("sql_type") String sqlType,
            @JsonProperty("field_type") String fieldType,
            @JsonProperty("column_keys") List<String> columnKeys,
            @JsonProperty("expr") String expr,
            @JsonProperty("presto_type") Type prestoType)
    {
        this.connectorId = requireNonNull(connectorId, "connector ID is null");
        this.ordinalPosition = ordinalPosition;
        this.name = requireNonNull(name, "name is null");
        this.columnKey = requireNonNull(columnKey, "column key is null");
        this.sqlType = requireNonNull(sqlType, "sqlType is null");
        this.fieldType = requireNonNull(fieldType, "fieldType is null");
        this.columnKeys = requireNonNull(columnKeys, "column keys is null");
        this.expr = requireNonNull(expr, "expr is null");
        this.prestoType = requireNonNull(prestoType, "presto type is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getColumnKey()
    {
        return columnKey;
    }

    @JsonProperty
    public String getSqlType()
    {
        return sqlType;
    }

    @JsonProperty
    public String getFieldType()
    {
        return fieldType;
    }

    @JsonProperty
    public List<String> getColumnKeys()
    {
        return columnKeys;
    }

    @JsonProperty
    public String getExpr()
    {
        return expr;
    }

    @JsonProperty
    public Type getPrestoType()
    {
        return prestoType;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new SchemalessColumnMetadata(getName(), getPrestoType(), getFieldType(), getColumnKey(), getSqlType(), getColumnKeys(), getExpr());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, ordinalPosition, name, columnKey, sqlType, fieldType, columnKeys, expr, prestoType);
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

        SchemalessColumnHandle other = (SchemalessColumnHandle) o;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.columnKey, other.columnKey) &&
                Objects.equals(this.sqlType, other.sqlType) &&
                Objects.equals(this.fieldType, other.fieldType) &&
                Objects.equals(this.columnKeys, other.columnKeys) &&
                Objects.equals(this.expr, other.expr) &&
                Objects.equals(this.prestoType, other.prestoType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("ordinalPosition", ordinalPosition)
                .add("name", name)
                .add("columnKey", columnKey)
                .add("sqlType", sqlType)
                .add("type", fieldType)
                .add("column keys", columnKeys)
                .add("expr", expr)
                .add("prestoType", prestoType)
                .toString();
    }
}
