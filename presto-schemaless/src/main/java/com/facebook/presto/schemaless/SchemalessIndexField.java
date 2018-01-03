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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a single field from a Schemlaess Index table
 * TODO  May need to re-adjust based on whats needed in Metadata. Also remove requireNonNull constrains for optional fields
 */
public class SchemalessIndexField
{
    private final String name;
    private final Type prestoType;
    private final String columnKey;
    private final String sqlType;
    private final String fieldType;
    private final List<String> columnKeys;
    private final String expr;

    @JsonCreator
    public SchemalessIndexField(
            @JsonProperty("name") String name,
            @JsonProperty("presto_type") Type prestoType,
            @JsonProperty("column_key") String columnKey,
            @JsonProperty("sql_type") String sqlType,
            @JsonProperty("field_type") String fieldType,
            @JsonProperty("column_keys") List<String> columnKeys,
            @JsonProperty("expr") String expr)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.prestoType = requireNonNull(prestoType, "prestoType is null");
        this.columnKey = requireNonNull(columnKey, "columnKey is null");
        this.sqlType = requireNonNull(sqlType, "sqlType is null");
        this.fieldType = requireNonNull(fieldType, "fieldType is null");
        this.columnKeys = requireNonNull(columnKeys, "columnKeys is null");
        this.expr = requireNonNull(expr, "expr is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getPrestoType()
    {
        return prestoType;
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

    SchemalessColumnHandle getColumnHandle(String connectorId, int index)
    {
        return new SchemalessColumnHandle(
                connectorId,
                index,
                getName(),
                getColumnKey(),
                getSqlType(),
                getFieldType(),
                getColumnKeys(),
                getExpr(),
                getPrestoType());
    }

    ColumnMetadata getColumnMetadata()
    {
        return new SchemalessColumnMetadata(getName(), getPrestoType(), getFieldType(), getColumnKey(), getSqlType(), getColumnKeys(), getExpr());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, prestoType, columnKey, sqlType, fieldType, columnKeys, expr);
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

        SchemalessIndexField other = (SchemalessIndexField) o;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.prestoType, other.prestoType) &&
                Objects.equals(this.columnKey, other.columnKey) &&
                Objects.equals(this.sqlType, other.sqlType) &&
                Objects.equals(this.fieldType, other.fieldType) &&
                Objects.equals(this.columnKeys, other.columnKeys) &&
                Objects.equals(this.expr, other.expr);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("prestoType", prestoType)
                .add("columnKey", columnKey)
                .add("sqlType", sqlType)
                .add("fieldType", fieldType)
                .add("column keys", columnKeys)
                .add("expr", expr)
                .toString();
    }
}
