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

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a row from Schemaless index table.
 * TODO Remove requireNonNull constrains for optional fields
 */
public class SchemalessIndexDefinition
{
    private final String kind;
    private final String alias;
    private final String state;
    private final String columnKey;
    private final String table;
    private final String shardField;
    private final String shardFieldExpr;
    private final Boolean mutableShardKeyEnabled;
    private final List<String> primaryKey;
    private final Boolean useAddedId;
    private final Long formatVersion;
    private final List<String> columnKeys;
    private final List<SchemalessIndexField> fields;
    private final List<List<String>> compositeIndexes;

    @JsonCreator
    public SchemalessIndexDefinition(
            @JsonProperty("kind") String kind,
            @JsonProperty("alias") String alias,
            @JsonProperty("state") String state,
            @JsonProperty("column_key") String columnKey,
            @JsonProperty("table") String table,
            @JsonProperty("shard_field_name") String shardField,
            @JsonProperty("shard_field_expr") String shardFieldExpr,
            @JsonProperty("mutable_shard_key_enabled") Boolean mutableShardKeyEnabled,
            @JsonProperty("primary_key") List<String> primaryKey,
            @JsonProperty("use_added_id") Boolean useAddedId,
            @JsonProperty("format_version") Long formatVersion,
            @JsonProperty("column_keys") List<String> columnKeys,
            @JsonProperty("fields") List<SchemalessIndexField> fields,
            @JsonProperty("composite_indexes") List<List<String>> compositeIndexes)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.alias = requireNonNull(alias, "alias is null");
        this.state = requireNonNull(state, "state is null");
        this.columnKey = requireNonNull(columnKey, "columnKey is null");
        this.table = requireNonNull(table, "table is null");
        this.shardField = requireNonNull(shardField, "shardField is null");
        this.shardFieldExpr = requireNonNull(shardFieldExpr, "shardFieldExpr is null");
        this.mutableShardKeyEnabled = requireNonNull(mutableShardKeyEnabled, "mutableShardKeyEnabled is null");
        this.primaryKey = requireNonNull(primaryKey, "primaryKey is null");
        this.useAddedId = requireNonNull(useAddedId, "useAddedId is null");
        this.formatVersion = requireNonNull(formatVersion, "formatVersion is null");
        this.columnKeys = requireNonNull(columnKeys, "columnKeys is null");
        this.fields = requireNonNull(fields, "fields is null");
        this.compositeIndexes = requireNonNull(compositeIndexes, "compositeIndexes is null");
    }

    @JsonProperty
    public String getKind()
    {
        return kind;
    }

    @JsonProperty
    public String getAlias()
    {
        return alias;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public String getColumnKey()
    {
        return columnKey;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getShardField()
    {
        return shardField;
    }

    @JsonProperty
    public String getShardFieldExpr()
    {
        return shardFieldExpr;
    }

    @JsonProperty
    public Boolean getMutableShardKeyEnabled()
    {
        return mutableShardKeyEnabled;
    }

    @JsonProperty
    public List<String> getPrimaryKey()
    {
        return primaryKey;
    }

    @JsonProperty
    public Boolean getUseAddedId()
    {
        return useAddedId;
    }

    @JsonProperty
    public Long getFormatVersion()
    {
        return formatVersion;
    }

    @JsonProperty
    public List<String> getColumnKeys()
    {
        return columnKeys;
    }

    @JsonProperty
    public List<SchemalessIndexField> getFields()
    {
        return fields;
    }

    @JsonProperty
    public List<List<String>> getCompositeIndexes()
    {
        return compositeIndexes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("kind", kind)
                .add("alias", alias)
                .add("state", state)
                .add("columnKey", columnKey)
                .add("table", table)
                .add("shard field", shardField)
                .add("shard field expr", shardFieldExpr)
                .add("mutable shard key enbaled", mutableShardKeyEnabled)
                .add("primary key", primaryKey)
                .add("use added ID", useAddedId)
                .add("format version", formatVersion)
                .add("column keys", columnKeys)
                .add("fields", fields)
                .add("composite indexes", compositeIndexes)
                .toString();
    }
}
